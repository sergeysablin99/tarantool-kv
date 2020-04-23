local log = require('log')

box.cfg{
  log_level = 5,
  log_format = 'json'
}

box.once('init', function()
	box.schema.create_space('map')
  box.space.map:format({
    {name= 'key', type = 'scalar'},
    {name = 'value', type = 'scalar'}
  })
	box.space.map:create_index('primary', {
      type = 'hash',
    	parts = {'key'} 
    })
end)


local json = require('json')
local map = box.space.map

-- Обработчик запросов к /kv
local function post_handler(req)
  is_valid_json, body = pcall(req.json, req)
  if not is_valid_json then
    log.error("%s %s %s bad request, invalid body",
      req:method(), req:path(), req:query())
    return {
      status = 400,
      headers = { ['content-type'] = 'text/html; charset=utf8' },
      body = [[
        <html>
          <body>Invalid body</body>
        </html>
      ]]
    }
  end

  local key = body['key']
  local value = body['value']

  if key == nil then
    log.error("%s %s %s bad request, empty 'key' field",
      req:method(), req:path(), req:query())
    return {
      status = 400,
      headers = { ['content-type'] = 'text/html; charset=utf8' },
      body = [[
        <html>
          <body>Empty field 'key'</body>
        </html>
      ]]
  }
  elseif value == nil then
    log.error("%s %s %s bad request, empty 'value' field",
      req:method(), req:path(), req:query())
    return {
      status = 400,
      headers = { ['content-type'] = 'text/html; charset=utf8' },
      body = [[
        <html>
          <body>Empty field 'value'</body>
        </html>
      ]]
  }
  else
    if map:get{key} ~= nil then
      log.error("%s %s %s bad request,  key %s already exists",
        req:method(), req:path(), req:query(), key)
      return {
      status = 409,
      headers = { ['content-type'] = 'text/html; charset=utf8' },
      body = [[
        <html>
          <body>Key already exists</body>
        </html>
      ]]
      }
    else 
      map:insert{key, value}
      log.info("%s %s %s OK, insert [%s, %s]",
        req:method(), req:path(), req:query() , key, value)
      return {
        status = 200,
        headers = { ['content-type'] = 'text/html; charset=utf8' },
        body = [[
          <html>
            <body>Ok</body>
          </html>
        ]]
      }
    end
  end
end

local function get_handler(req)
  local offset = string.len("/kv/") + 1
  local key = string.sub(req:path(), offset)
  value = map:get{key}
  if value == nil then
    log.error("%s %s %s bad request, key %s not found",
      req:method(), req:path(), req:query() , key)
    return {
      status = 404,
      headers = { ['content-type'] = 'text/html; charset=utf8' },
      body = [[
        <html>
          <body>Key not found</body>
        </html>
      ]]
    }
  else
    log.info("%s %s %s OK, return value %s",
      req:method(), req:path(), req:query() , value)
    return {
      status = 200,
      headers = { ['content-type'] = 'text/html; charset=utf8' },
      body = value[2]
    }
  end
end

local function del_handler(req)
  local offset = string.len("/kv/") + 1
  local key = string.sub(req:path(), offset)
  value = map:get{key}
  if value == nil then
    log.error("%s %s %s bad request, key %s not found",
      req:method(), req:path(), req:query() , key)
    return {
      status = 404,
      headers = { ['content-type'] = 'text/html; charset=utf8' },
      body = [[
        <html>
          <body>Key not found</body>
        </html>
      ]]
    }
  else
    map:delete(key)
    log.info("%s %s %s OK, deleted key %s %s",
      req:method(), req:path(), req:query() , key)
    return {
      status = 200,
      headers = { ['content-type'] = 'text/html; charset=utf8' },
      body = [[
        <html>
          <body>OK</body>
        </html>
      ]]
    }
  end
end

local function put_handler(req)
  is_valid_json, body = pcall(req.json, req)
  if not is_valid_json then
    log.error("%s %s %s bad request, invalid body",
      req:method(), req:path(), req:query())
    return {
      status = 400,
      headers = { ['content-type'] = 'text/html; charset=utf8' },
      body = [[
        <html>
          <body>Invalid body</body>
        </html>
      ]]
    }
  end

  local offset = string.len("/kv/") + 1
  local key = string.sub(req:path(), offset)
  local value = body['value']

  if value == nil then
    log.error("%s %s %s bad request, empty 'value' field",
      req:method(), req:path(), req:query())
    return {
      status = 400,
      headers = { ['content-type'] = 'text/html; charset=utf8' },
      body = [[
        <html>
          <body>Bad request</body>
        </html>
      ]]
  }
  else
    if map:get{key} == nil then
      log.error("%s %s %s bad request, key %s not exists",
      req:method(), req:path(), req:query(), key)
      return {
        status = 404,
        headers = { ['content-type'] = 'text/html; charset=utf8' },
        body = [[
          <html>
            <body>Key not exists</body>
          </html>
        ]]
      }
    else 
      map:put({key, value})
      log.info("%s %s %s OK,reset [%s, %s]",
      req:method(), req:path(), req:query() , key, value)
      return {
        status = 200,
        headers = { ['content-type'] = 'text/html; charset=utf8' },
        body = [[
          <html>
            <body>Ok</body>
          </html>
        ]]
      }
    end
  end
end

local httpd = require('http.server')
local server = httpd.new('10.128.0.2', 8080)
local router = require('http.router').new({charset = 'application/json'})
server:set_router(router)
router:route({ path = '/kv', method = 'POST'  }, post_handler)
router:route({ path = '/kv/:id', method = 'GET'  }, get_handler)
router:route({ path = '/kv/:id', method = 'DELETE'  }, del_handler)
router:route({ path = '/kv/:id', method = 'PUT'  }, put_handler)
server:start()