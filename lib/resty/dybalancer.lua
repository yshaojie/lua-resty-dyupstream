local _M = {}
local balancer = require "ngx.balancer"
local dyupstream = require "dyupstream"

function _M.election_peer(server_name)
    local status, code = balancer.get_last_failure()
    if not status then --the first attempt
        --Note that the current attempt is excluded in the count number set here.
        local ok, err = balancer.set_more_tries(2)
        if not ok then
            ngx.log(ngx.ERR, "set_more_tries failed, ", err)
        end
    end

    if status == "failed" then --the last peer fail.
        local last_peer = ngx.ctx.last_peer
        if last_peer then
            dyupstream.add_fail_peer(server_name,last_peer)
        end

    end

    local peer, err = dyupstream.round_robin_server(server_name)
    if not peer then
        ngx.log(ngx.ERR, "select peer failed, ", err)
        return
    end


    ngx.ctx.last_peer = peer
    local ok, err = balancer.set_current_peer(peer.host, peer.port)
    if not ok then
        ngx.log(ngx.ERR, "set_current_peer failed, ", err)
    end
end

return _M



