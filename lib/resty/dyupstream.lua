local _M = {}
local http = require "resty.http"
local cjson_safe = require "cjson.safe"
local lock = require "resty.lock"
local ngx_timer_at = ngx.timer.at
local ngx_log = ngx.log
local ngx_ERR = ngx.ERR
local ngx_worker_exiting = ngx.worker.exiting
--etcd host list
local etcd_node_list = {}
--all server list exam:{"server-name":{"count":0,"nodes":{}}}
local servers = {}
--request fail server list,exam:{"server-name":{"nodes":{}}}
local fail_servers ={}
local function log(message)
    ngx_log(ngx_ERR, message)
end

string.split = function(s, p)
    local rt = {}
    string.gsub(s, '[^' .. p .. ']+', function(w) table.insert(rt, w) end)
    return rt
end

local function get_lock(lock_name)
    local lock, err = lock:new(_M.conf.dict)
    if err then
        log("get lock error:" .. err)
        return
    end
    local elapsed, err = lock:lock(lock_name)
    return lock;
end


local function open_dump_file(mode)
    local dump_path = _M.conf.dump_path .. "/" .. _M.conf.etcd_path:gsub("/", "_") .. ".json"
    local dump_file, err = io.open(dump_path, mode)
    if not io.type(dump_file) then --file not exist
        os.execute("mkdir -p  " .. _M.conf.dump_path)
        --touch file
        os.execute("touch " .. dump_path)
        dump_file, err = io.open(dump_path, mode)
    end
    return dump_file, err;
end

local function dump_tofile()
    local saved = false
    log("start dump file")
    while not saved do
        local lock = get_lock("dump_file_lock")
        if lock then
            local file, err = open_dump_file('w')
            if file == nil then
                lock:unlock()
                log(err)
                return false
            end

            local data = cjson_safe.encode(servers)
            file:write(data)
            file:flush()
            file:close()
            saved = true
            log("dump file success. data:" .. data)
            lock:unlock()
        end
    end
end

local function query_etcd_data(uri)
    local http_conn = http.new()
    http_conn:set_timeout(3000)
    for index, node in pairs(etcd_node_list) do
        http_conn:connect(node.host, node.port)
        local http_address = "http://"..node.host..":"..node.port..uri
        local res, err = http_conn:request_uri(http_address,{method = "GET"})
        if res then
            local json_body = cjson_safe.decode(res.body)
            --errorCode=100 ->	“Key not found”
            if json_body then
                return json_body
            end
        end
    end
    log("query etcd data fail. uri="..uri)
    return nil
end

local function update_server(name)
    log("start update server_name=[" .. name .. "] server node list.")
    --server register url
    local server_register_url = "/v2/keys" .. _M.conf.etcd_path .. name
    local json_body = query_etcd_data(server_register_url)
    local nodes = {}
    --not exist server or no server node
    if not json_body or json_body.errorCode or not json_body.node.nodes then
        --do nothing
        return
    end

    --for each server list
    for index, server_node in pairs(json_body.node.nodes) do
        local data, err = cjson_safe.decode(server_node.value)
        repeat
            if not data then
                break
            end
            if not data.host or not data.port then
                log("illegal server register data:"..server_node.value)
                break
            end

            local weight = data.weight or 1
            --insert the server info to server list
            table.insert(nodes, { host = data.host, port = data.port, weight = weight})
            break
        until true
    end

    if not servers[name] then --not exist ,init it
        servers[name] = {}
    end

    servers[name].nodes = nodes
    servers[name].count = table.getn(nodes)
    ngx.log(ngx.ERR, "update [" .. name .. "] nodes, data=" .. cjson_safe.encode(servers[name]))
    dump_tofile()
    log("update server_name=[" .. name .. "] server node list success.")
end


--load server config from local file
local function load_from_local()
    local file, err = open_dump_file("r")
    if not file then
        log("dump file not exist,load config fail.")
        return
    end
    local d, err = file:read("*a")
    if err then
        log("read data from dump file fail. error:" .. err)
        file:close()
        return
    end
    log("load config data from local, data:" .. d)
    local nodes = cjson_safe.decode(d)
    servers = nodes
    file:close()
end

local function init_servers()
    local s_url = "/v2/keys" .. _M.conf.etcd_path
    local data_json = query_etcd_data(s_url)
    if data_json then
        --for each server list
        for n, node in pairs(data_json.node.nodes) do
            local _, end_index = string.find(node.key, _M.conf.etcd_path, 1, true)
            local server_name = string.sub(node.key, end_index + 1, -1)
            update_server(server_name)
        end
    else
        load_from_local()
    end
end

local function proccess_action(message)
    log("INFO: recieve change: " .. message)

    local change = cjson_safe.decode(message)
    --error message
    if change.errorCode then
        log("message has errorCode="..change.errorCode.." skip this message")
        return
    end

    local action = change.action
    local strs = string.split(change.node.key,"/")
    if change.node.dir then
        --key style /servers/server-name
        if table.getn(strs) ~= 2 then
            log("illegal etcd path,etcd_path=" .. change.node.key)
            return
        end
        local server_name = strs[2]
        if action == "delete" then
            servers[server_name] = nil
        else
            update_server(server_name)
        end
    else
        --key style /servers/server-name/localhost:8080
        if table.getn(strs) ~= 3 then
            log("illegal etcd path,etcd_path=" .. change.node.key)
            return
        end
        update_server(strs[2])
    end
    dump_tofile()
end

local function do_watch()
    local etcd_watch_http_connect
    --get a effective connection
    for _, node in pairs(etcd_node_list) do
        etcd_watch_http_connect = http.new()
        etcd_watch_http_connect:set_timeout(60000)
        local ok, err = etcd_watch_http_connect:connect(node.host, node.port)
        if ok then
            break
        else
            log("conn " .. node.host .. ":" .. node.port .. " fail. error=" .. err)
            etcd_watch_http_connect = nil
        end
    end

    --no connection available
    if not etcd_watch_http_connect then
        log("no available etcd nodes")
        return
    end

    local url = "/v2/keys" .. _M.conf.etcd_path .. "?wait=true&recursive=true"
    if _M.etcd_index then
        url = url .. "&waitIndex=" .. (_M.etcd_index + 1)
    end
    local res, err = etcd_watch_http_connect:request({ path = url, method = "GET" })
    if err then
        return
    end
    local body, err = res:read_body()
    etcd_watch_http_connect:close()
    if err or res.status ~= 200 then
        return
    end

    local etcd_index = res.headers['X-Etcd-Index'];
    --store the max etcd index
    if not _M.etcd_index or (etcd_index and _M.etcd_index < etcd_index) then
        _M.etcd_index = etcd_index
    end
    if body then
        proccess_action(body)
    end
end

local function watch(premature)
    log("watch start...")
    if premature then
        log("time work premature")
        return
    end
    while true do
        if ngx_worker_exiting() then
            log("work exit,stop watch etcd")
            return
        end
        do_watch()
    end
end

local function check_fail_nodes()
    while true do
        if ngx_worker_exiting() then
            log("work exit,stop check_fail_nodes")
            return
        end
        for server_name, data in pairs(fail_servers) do
            for index, node in pairs(data.nodes) do
                local current_time = os.time()
                if current_time-node.first_time >= node.fail_timeout then
                    --remove from fail_servers
                    table.remove(data.nodes,index)
                    local server_register_url = "/v2/keys" .. _M.conf.etcd_path .. server_name.."/"..node.host..":"..node.port
                    local json_body = query_etcd_data(server_register_url)

                    --server exist
                    if json_body and not json_body.errorCode then
                        if not servers[server_name] then
                            servers[server_name]={}
                            servers[server_name].nodes={}
                        end
                        local exist = false
                        for index, available_node in pairs(servers[server_name].nodes) do
                            --has been existed
                            if available_node.host == node.host and available_node.port == node.port then
                                exist = true
                                break
                            end
                        end

                        --add to zhe server_name
                        if not exist then
                            table.insert(servers[server_name].nodes,node.bak_data)
                        end

                    end
                end
            end
        end
        ngx.sleep(1)
    end
end


function _M.init(conf)
    -- Load the upstreams from file
    log("start init dyupstream env")
    --format etcd patch
    conf.etcd_path = string.gsub(conf.etcd_path .. "/", "//", "/")
    _M.conf = conf
    --init etcd cluster list
    etcd_node_list = {}
    servers = {}
    for _, url in pairs(string.split(_M.conf.etcd_urls, ",")) do
        local host, port = url:match("(.*):([0-9]+)")
        local node = {}
        node.host = host
        node.port = port
        table.insert(etcd_node_list, node)
    end
    --init server configs
    ngx_timer_at(0, init_servers)
    -- Start the etcd watcher
    ngx_timer_at(0, watch)
    -- fail node check
    ngx_timer_at(0, check_fail_nodes)

    log("end init dyupstream env success")
end

-- Round robin
function _M.round_robin_server(name)

    if not servers[name] then
        return nil, "upstream not ready."
    end
    local dict = ngx.shared[_M.conf.dict]
    local robin_key = name .. "_count"
    local count, err = dict:incr(robin_key, 1)
    if not count then --incr fail
        count = 1
        local ok = dict:set(robin_key, 1)
        if not ok then
            return _M.random_server(name)
        end
    end
    local index = count % #servers[name].nodes + 1
    return servers[name].nodes[index]
end

function _M.random_server(name)
    local server_count = servers[name].count
    if not servers[name] or not server_count or server_count < 1 then
        return nil, "upstream not ready."
    end
    local server_count = servers[name].count
    local index = math.random(1, server_count)
    local peer = {}
    peer.host = servers[name].nodes[index].host
    peer.port = servers[name].nodes[index].port
    return peer
end

function _M.add_fail_peer(server_name,peer)
    local fail_node;
    if fail_servers[server_name] and fail_servers[server_name].nodes then
        for index, node in pairs(fail_servers[server_name].nodes) do
            if node.host == peer.host and node.port == peer.port then
                fail_node = node;
                break;
            end
        end
    end

    local server_node;
    for index, node in pairs(servers[server_name].nodes) do
        if node.host == peer.host and node.port == peer.port then
            server_node = node
        end
    end

    if not server_node then
        return
    end

    --init zhe fail node
    if not fail_node then
        fail_node = {}
        fail_node.first_time = os.time()
        fail_node.host = server_node.host
        fail_node.port = server_node.port
        fail_node.fail_count=0
        fail_node.max_fails = server_node.max_fails or 3
        fail_node.fail_timeout = server_node.fail_timeout or 10
        fail_node.bak_data=server_node
        if not fail_servers[server_name] then
            fail_servers[server_name] = {}
            fail_servers[server_name].nodes = {}
        end
        table.insert(fail_servers[server_name].nodes,fail_node)
    end

    fail_node.fail_count = fail_node.fail_count+1
    --remove zhe fail node from servers
    if fail_node.fail_count >= fail_node.max_fails then
        for index, node in pairs(servers[server_name].nodes) do
            if node.host == server_node.host and node.port == server_node.port then
                table.remove(servers[server_name].nodes,index)
            end
        end
    end

    servers[server_name].count = table.getn(servers[server_name].nodes)
end

function _M.all_servers(name)
    if servers[name] then
        return servers[name].nodes
    else
        return {}
    end
end

return _M