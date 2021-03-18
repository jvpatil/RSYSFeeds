

from user_agents import parse

# iPhone's user agent string
# ua_string = 'Mozilla/5.0 (iPhone; CPU iPhone OS 5_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9B179 Safari/7534.48.3'
# ua_string = 'Mozilla/5.0 (Windows NT 5.1; rv:11.0) Gecko Firefox/11.0 (via ggpht.com GoogleImageProxy)'
ua_string = 'Mozilla/5.0 (Linux; Android 9; KFKAWI Build/PS7318; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/87.0.4280.101 Safari/537.36'
user_agent = parse(ua_string)

# Accessing user agent's browser attributes
print("Browser Family :: " ,user_agent.browser)# returns Browser(family=u'Mobile Safari', version=(5, 1), version_string='5.1')
print(user_agent.browser.family)  # returns 'Mobile Safari'
print(user_agent.browser.version)  # returns (5, 1)
print(user_agent.browser.version_string)   # returns '5.1'

# Accessing user agent's operating system properties
print(user_agent.os)  # returns OperatingSystem(family=u'iOS', version=(5, 1), version_string='5.1')
print(user_agent.os.family)  # returns 'iOS'
print(user_agent.os.version)  # returns (5, 1)
print(user_agent.os.version_string ) # returns '5.1'

# Accessing user agent's device properties
print(user_agent.device)  # returns Device(family=u'iPhone', brand=u'Apple', model=u'iPhone')
print(user_agent.device.family)  # returns 'iPhone'
print(user_agent.device.brand) # returns 'Apple'
print(user_agent.device.model) # returns 'iPhone'

# Viewing a pretty string version
print(str(user_agent)) # returns "iPhone / iOS 5.1 / Mobile Safari 5.1"

