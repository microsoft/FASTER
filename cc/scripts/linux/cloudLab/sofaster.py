"""
Allocate a cluster of CloudLab machines to run SoFASTER.

Instructions:
"""

import geni.urn as urn
import geni.portal as portal
import geni.rspec.pg as rspec
import geni.aggregate.cloudlab as cloudlab

# The possible set of base disk-images that this cluster can be booted with.
# The second field of every tupule is what is displayed on the cloudlab
# dashboard.
images = [ ("UBUNTU16-64-STD", "Ubuntu 16.04 (64-bit)") ]

# The possible set of node-types this cluster can be configured with.
nodes = [
        ("c6420", "c6420 (2 x 16 core Xeon Gold 6142, 384 GB DDR4 RAM," +\
        " 10 Gbps Intel Ethernet, 1 TB HDD)"),
        ("c6220", "c6220 (2 x 8 core Xeon E5 2650v2, 64 GB DDR3 RAM," +\
        " 40 Gbps Mellanox CX3, 1 TB HDD)"),
        ("r320", "r320 (1 x 8 core Xeon E5 2650v2, 16 GB DDR3 RAM," +\
        " 40 Gbps Mellanox CX3, 4 x 500 GB HDD)"),
        ]

# Allows for general parameters like disk image to be passed in. Useful for
# setting up the cloudlab dashboard for this profile.
context = portal.Context()

# Default the disk image to 64-bit Ubuntu 16.04
context.defineParameter("image", "Disk Image",
        portal.ParameterType.IMAGE, images[0], images,
        "Specify the base disk image that all the nodes of the cluster " +\
        "should be booted with.")

# Default the node type to the c6420.
context.defineParameter("type", "Node Type",
        portal.ParameterType.NODETYPE, nodes[0], nodes,
        "Specify the type of nodes the cluster should be configured with. " +\
        "For more details, refer to " +\
        "\"http://docs.cloudlab.us/hardware.html#%28part._apt-cluster%29\"")

# Default the cluster size to 1 node.
context.defineParameter("size", "Cluster Size",
        portal.ParameterType.INTEGER, 1, [],
        "Specify the size of the cluster." +\
        "To check availability of nodes, visit " +\
        "\"https://www.cloudlab.us/cluster-graphs.php\"")

params = context.bindParameters()

request = rspec.Request()

# Create a local area network over 10 Gbps.
lan = rspec.LAN()
lan.bandwidth = 10000000 # This is in kbps.

# Setup node names.
aliases = []
for i in range(params.size):
    aliases.append("sofaster%02d" % (i + 1))

# Setup the cluster one node at a time.
for i in range(params.size):
    node = rspec.RawPC(aliases[i])

    node.hardware_type = params.type
    node.disk_image = urn.Image(cloudlab.Utah, "emulab-ops:%s" % params.image)

    bs = node.Blockstore("bs" + str(i), "/scratch")
    bs.size = "1024GB"

    request.addResource(node)

    # Add this node to the LAN.
    if params.size > 1:
        iface = node.addInterface("eth0")
        lan.addInterface(iface)

# Add the lan to the request.
request.addResource(lan)

# Generate the RSpec
context.printRequestRSpec(request)
