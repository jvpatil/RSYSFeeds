from lxml import etree

# custom_property = "<props><prop name = 'FirstName' value='Jagan' /><prop name = 'LastName' value='Patil' /></props>"
# custom_property = "<props><prop name = ""FirstName"" value=""Jagan""/><prop name = ""LastName"" value=""Patil"" /></props>"
def read_custom_properties(cust_prop_xml):
    custPropertyDict = {}
    root = etree.fromstring(cust_prop_xml)
    props = root.findall('prop')
    for i in range(len(props)):
        name = props[i].get('name')
        value = props[i].get('value')
        # print(name,value)
        custPropertyDict[name] = value
    print(custPropertyDict)

# if __name__ == '__main__':
    # get_attributes(custom_property)
    # main()