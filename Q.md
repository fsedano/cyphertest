match (d:Device)-[]-(:Interface)-[]-(:InterfaceHub)-[]-(:Hub)-[]-(:InterfaceHub)-[]-(:Interface)-[]-(d2:Device) return p limit 1
