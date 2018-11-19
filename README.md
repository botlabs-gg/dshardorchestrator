dshardorchestrator provides a sharding orchestrator for discord bots.

It's purpose is to manage nodes and assign shards to them, aswell as migrating shards between the nodes, which is one method of scaling large discord bots, spreading shards across processes and servers.

I would not recommend using this, as 1. it currently only works against my discordgo fork and 2. its lackign a lot of tests still (although this im improving on)

It's going to be used in YAGPDB and thats also the main purpose i made it for.

# Pitfalls

Currently its somewhat easy to break, if you try to break it that is, im working towards that but yeah, in it's current state i would just not reccomend using it.

Essentials TODO:

 - Full shard migration
 - Monitor making sure all shards are up
 - Make sure disconnected nodes are handled correctly

 - Add in safeguards for doing things like, stopping nodes in the middle of migration.
Later:
 - Extended status polling form nodes