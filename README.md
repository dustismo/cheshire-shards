goshire-shards
==============


The goal of this project is to make balanced horizontally scalable Goshire (https://github.com/trendrr/goshire) services simple to to write. 

It consists of three pieces:

### Goshire service
   This is the piece that you write.  To use the shards project you must implement the PartitionManager interface and register the default controllers. Examples and more coming soon..
   
### Admin
   This is the admin page where you add/remove nodes from your cluster.  This needs to be operational in order to rebalance the cluster.  It does *NOT* need to be available for the normal operation of your cluster.
   
### Router
   This process handles routing requests to the appropriate node(s) in the cluster.  In a typical deployment you would run a router on every server that connects to the cluster.  (i.e. your apps always connect to localhost).


====================

Note this is in active development and not at all ready for prime time.

