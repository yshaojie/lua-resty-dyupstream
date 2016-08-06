local _M = {}
local http = require "resty.http"
local cjson_safe = require "cjson.safe"
local lock = require "resty.lock"
local ngx_timer_at = ngx.timer.at
local ngx_log = ngx.log
local ngx_ERR = ngx.ERR
local ngx_sleep = ngx.sleep
local ngx_worker_exiting = ngx.worker.exiting

--servers data
_M.data = {}
--etcd cluster node list
_M.etcd_node_list = {}
local function log(c)
    ngx_log(ngx_ERR, c)
end

string.split = function(s, p)
    local rt = {}
    string.gsub(s, '[^' .. p .. ']+', function(w) table.insert(rt, w) end)
    return rt
end

local function split_addr(s)
    host, port = s:match("(.*):([0-9]+)")

    -- verify the port
    local p = tonumber(port)
    if p == nil then
        return "127.0.0.1", 0, "port invalid"
    elseif p < 1 or p > 65535 then
        return "127.0.0.1", 0, "port invalid"
    end

    -- verify the ip addr
    local chunks = { host:match("(%d+)%.(%d+)%.(%d+)%.(%d+)") }
    if (#chunks == 4) then
        for _, v in pairs(chunks) do
            if (tonumber(v) < 0 or tonumber(v) > 255) then
                return "127.0.0.1", 0, "host invalid"
            end
        end
    else
        return "127.0.0.1", 0, "host invalid"
    end

    -- verify pass
    return host, port, nil
end

local function get_lock()
    local dict = _M.conf.dict
    local key = "lock"
    -- only the worker who get the lock can update the dump file.
    local ok, err = dict:add(key, true)
    if not ok then
        if err == "exists" then
            return nil
        end
        log("failed to add key \"", key, "\": ", err)
        return nil
    end
    return true
end

local function release_lock()
    local dict = _M.conf.dict
    local key = "lock"
    local ok, err = dict:delete(key)
    return true
end


local function open_dump_file(mode)
    local dump_path = _M.conf.dump_path.. "/" .. _M.conf.etcd_path:gsub("/", "_") .. ".json"
    log("open file:"..dump_path)
    local dump_file, err = io.open(dump_path, mode)
    if not io.type(dump_file) then --file not exist
        os.execute("mkdir -p  ".._M.conf.dump_path)
        --touch file
        os.execute("touch ".. dump_path)
        dump_file, err = io.open(dump_path, mode)
    end
    return dump_file,err;
end

local function dump_tofile()
    local saved = false
    log("start dump file")
    while not saved do
        local locks ,err = lock:new(_M.conf.dict)
        if err then
            log("get locks error:"..err)
        end
        local elapsed,err = locks:lock("dump_file_lock")
        if not err then
            local file, err = open_dump_file('w')
            if file == nil then
                locks:unlock()
                log(err)
                return false
            end

            local data = cjson_safe.encode(_M.data)
            file:write(data)
            file:flush()
            file:close()
            saved = true
            log("dump file success. data:"..data)
            locks:unlock()
        else
            log("wait for lock fail. err:"..err)
        end
    end
end

local function query_etcd_data(url)
    log("query etcd data ,url="..url)
    local http_conn = http.new()
    http_conn:set_timeout(1000)
    for index, node in pairs(_M.etcd_node_list) do
        http_conn:connect(node.host, node.port)
        local res, err = http_conn:request({ path = url, method = "GET" })
        if not err then
            local body, err = res:read_body()
            ngx.log(ngx.ERR, body)
            if not err then
                local json_body = cjson_safe.decode(body)
                --errorCode=100 ->	“Key not found”
                if json_body then
                    return json_body
                end
            end
        end
    end
    return nil
end

local function update_server(name)
    log("start update server_name=[" .. name .. "] server node list.")
    --server register url
    local server_register_url = "/v2/keys" .. _M.conf.etcd_path .. name .. "?recursive=true"
    local json_body = query_etcd_data(server_register_url)
    local servers = {}

    --not exist server or no server node
    if json_body.errorCode or not json_body.node.nodes then
        --do nothing
    else
        --for each server list
        for index, server_node in pairs(json_body.node.nodes) do
            local data, err = cjson_safe.decode(server_node.value)
            if not err then
                --insert the server info to server list
                table.insert(servers, { host = data.host, port = data.port, weight = data.weight, current_weight = 0 })
            end
        end
    end

    if not _M.data[name] then --not exist ,init it
    _M.data[name] = {}
    end

    _M.data[name].servers = servers
    _M.data[name].count = table.getn(servers)
    ngx.log(ngx.ERR, "update [" .. name .. "] servers, data=" .. cjson_safe.encode(_M.data[name]))
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
    local d ,err= file:read("*a")
    if err then
        log("read data from dump file fail. error:"..err)
        file:close()
        return
    end
    log("load config data from local, data:"..d)
    local data = cjson_safe.decode(d)
    _M.data = data
    file:close()
end

local function init_servers()
    local s_url = "/v2/keys" .. _M.conf.etcd_path .. "?recursive=true"
    local data_json = query_etcd_data(s_url)
    if data_json then
        --for each server list
        for n, node in pairs(data_json.node.nodes) do
            local _,end_index=string.find(node.key,_M.conf.etcd_path,1,true)
            local server_name = string.sub(node.key,end_index+1,-1)
            update_server(server_name)
        end
    else
        load_from_local()
    end
end


local function watch(premature)
    if premature then
        log("time work premature")
        return
    end

    if ngx_worker_exiting() then
        log("work exit")
        return
    end

    local http_connect = http:new()
    http_connect:set_timeout(60000)

    --get a effective connection
    for _, node in pairs(_M.etcd_node_list) do
        local ok, err = http_connect:connect(node.host, node.port)
        if ok then
            break
        end
    end
    --todo no connection available?

    local url = "/v2/keys" .. _M.conf.etcd_path .. "?wait=true&recursive=true"
    local res, err = http_connect:request({ path = url, method = "GET" })
    if not err then
        local body, err = res:read_body()
        http_connect:close()
        --restart watch
        ngx_timer_at(0,watch)
        --        ngx_timer_at(0, watch)
        if not err then
            log("INFO: recieve change: " .. body)
            local change = cjson_safe.decode(body)
            if not change.errorCode then
                local action = change.action
                local key = change.node.key
                if change.node.dir then
                    --key style /servers/server-name
                    local _, count = string.gsub(key, "/", "/")
                    if count ~= 2 then
                        log("illegal etcd path,etcd_path="..key)
                        return
                    end
                    local _,end_index=string.find(key,_M.conf.etcd_path,1,true)
                    local server_name = string.sub(key,end_index+1,-1)
                    if action == "delete" then
                        _M.data[server_name] = nil
                    else
                        update_server(server_name)
                    end
                else
                    local _, count = string.gsub(key, "/", "/")
                    if count ~= 3 then
                        log("illegal etcd path,etcd_path="..key)
                        return
                    end
                    local _,end_index=string.find(key,_M.conf.etcd_path,1,true)
                    local server_path = string.sub(key,end_index+1,-1)
                    local _,_,server_name,node = server_path:find("(.*)/(.*)")
                    update_server(server_name)
                end
            end
            dump_tofile()
        end
    else
        ngx_timer_at(0, watch)
    end
    return
end

function _M.init(conf)
    -- Load the upstreams from file
    log("start init dyupstream env")
    --format etcd patch
    conf.etcd_path = string.gsub(conf.etcd_path.."/","//", "/")
    _M.conf = conf
    --init etcd cluster list
    _M.etcd_node_list = {}
    log(_M.conf.etcd_urls)
    for _, url in pairs(string.split(_M.conf.etcd_urls, ",")) do
        local host, port = url:match("(.*):([0-9]+)")
        local node = {}
        node.host = host
        node.port = port
        table.insert(_M.etcd_node_list, node)
    end
    --init server configs
    ngx.timer.at(0, init_servers)
    log("end init dyupstream env success")

    -- Start the etcd watcher
    ngx_timer_at(0, watch)
end

-- Round robin
function _M.round_robin_server(name)

    if not _M.data[name] then
        return nil, "upstream not ready."
    end

    local c = ngx.shared[_M.conf.dict]
    local robin_key = name .. "_count"
    local count, err = c:incr(robin_key, 1)
    if not count then --incr fail
        count = 1
        local ok = c:set(robin_key, 1)
        if not ok then
            return random_server(name)
        end
    end
    local index = count % #_M.data[name].servers+1
    return _M.data[name].servers[index]
end

function _M.random_server(name)
    local server_count = _M.data[name].count
    if not _M.data[name] or not server_count or server_count<1 then
        return nil, "upstream not ready."
    end
    local server_count = _M.data[name].count
    local index = math.random(1,server_count)
    return _M.data[name].servers[index]
end

function _M.round_robin_with_weight(name)
    if not _M.ready or not _M.data[name] then
        return nil, "upstream not ready."
    end

    local peers = _M.data[name].servers
    local total = 0
    local pick = nil

    for _, peer in pairs(peers) do

        -- If no weight set, the default is 1.
        if peer.weight == nil then
            peer.weight = 1
        end

        if peer.current_weight == nil then
            peer.current_weight = 0
        end

        if peer.weight == 0 then
            goto continue
        end

        peer.current_weight = peer.current_weight + peer.weight
        total = total + peer.weight

        if pick == nil or pick.current_weight < peer.current_weight then
            pick = peer
        end

        :: continue ::
        end

        pick.current_weight = pick.current_weight - total

        return pick
    end

    function _M.all_servers(name)
        if _M.data[name] then
            return _M.data[name].servers
        else
            return nil
        end
    end

    return _M