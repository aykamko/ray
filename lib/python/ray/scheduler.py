import argparse
import random
import redis
import time

parser = argparse.ArgumentParser(description="Parse addresses for the global scheduler to connect to.")
parser.add_argument("--redis-address", required=True, type=str, help="the port to use for Redis")

# This is a dictionary mapping object IDs to a list of the nodes containing
# that object ID.
cached_object_table = {}
# This is a dictionary mapping function IDs to a list of the nodes (or
# workers?) that can execute that function.
cached_function_table = {}
cached_function_info = {}
cached_task_info = {}
cached_workers = []

cached_worker_info = {}

# Replace this with a deque.
unscheduled_tasks = []

num_workers = 0

available_workers = []

def function_id_and_dependencies(task_info):
  #print "task_info is {}".format(task_info)
  function_id = task_info["function_id"]
  dependencies = []
  i = 0
  while True:
    if "arg:{}:id".format(i) in task_info:
      dependencies.append(task_info["arg:{}:id".format(i)])
    elif "arg:{}:val".format(i) not in task_info:
      break
    i += 1
  return function_id, dependencies

def can_schedule(worker_id, task_id):
  task_info = cached_task_info[task_id]
  function_id = task_info["function_id"]
  if not cached_function_table.has_key(function_id):
    #print "Function {} is not in cached_function_table.keys()".format(function_id)
    return False
  if cached_worker_info[worker_id]["export_counter"] < cached_function_info[function_id]["export_counter"]:
    return False
  if worker_id not in cached_function_table[function_id]:
    return False
  for obj_id in task_info["dependencies"]:
    if not cached_object_table.has_key(obj_id):
      return False
  return True

if __name__ == "__main__":
  args = parser.parse_args()

  redis_host, redis_port = args.redis_address.split(":")
  redis_port = int(redis_port)

  redis_client = redis.StrictRedis(host=redis_host, port=redis_port)
  redis_client.config_set("notify-keyspace-events", "AKE")
  pubsub_client = redis_client.pubsub()

  # Messages published after the call to pubsub_client.psubscribe and before the
  # call to pubsub.listen should be received in the pubsub_client.listen loop.
  pubsub_client.psubscribe("*")

  # Get anything we may have missed.
  # remote functions
  # objects (there shouldn't be any)
  # tasks
  # workers


  # Receive messages and process them.
  for msg in pubsub_client.listen():
    #print msg
    # Update cached data structures.
    if msg["channel"].startswith("__keyspace@0__:Object:"):
      # Update the cached object table.
      object_key = msg["channel"].split(":", 1)[1]
      obj_id = object_key.split(":", 1)[1]
      cached_object_table[obj_id] = redis_client.lrange(object_key, 0, -1)
      #print "Object Table is {}".format(cached_object_table)
    elif msg["channel"] == "__keyspace@0__:GlobalTaskQueue" and msg["data"] == "rpush":
      # Update the list of unscheduled tasks and the cached task info.
      #print "GlobalTaskQueue is {}".format(redis_client.lrange("GlobalTaskQueue", 0, -1))
      task_id = redis_client.lpop("GlobalTaskQueue")
      unscheduled_tasks.append(task_id)
      task_key = "graph:{}".format(task_id)
      #print "Task key is {}".format(task_key)
      task_info = redis_client.hgetall(task_key)
      function_id, dependencies = function_id_and_dependencies(task_info)
      cached_task_info[task_id] = {"function_id": function_id,
                                   "dependencies": dependencies}
      #print "Cached Task Info is {}".format(cached_task_info)
    elif msg["channel"] == "__keyspace@0__:Workers":
      worker_id = redis_client.lindex("Workers", num_workers)
      print "Adding worker {}".format(worker_id)
      cached_workers.append(worker_id)
      available_workers.append(worker_id)
      cached_worker_info[worker_id] = {"export_counter": 0}
      num_workers += 1
    elif msg["channel"].startswith("__keyspace@0__:RemoteFunction:"):
      key = msg["channel"].split(":", 1)[1]
      function_id = key.split(":", 1)[1]
      function_export_counter = int(redis_client.hget(key, "export_counter"))
      num_return_vals = int(redis_client.hget(key, "num_return_vals"))
      cached_function_info[function_id] = {"export_counter": function_export_counter,
                                           "num_return_vals": num_return_vals}
    elif msg["channel"].startswith("__keyspace@0__:FunctionTable"):
      function_table_key = msg["channel"].split(":", 1)[1]
      function_id = function_table_key.split(":", 1)[1]
      if not cached_function_table.has_key(function_id):
        cached_function_table[function_id] = []
      worker_id = redis_client.lindex(function_table_key, len(cached_function_table[function_id]))
      cached_function_table[function_id].append(worker_id)
      #print "Adding worker {} to function table for function {}".format(worker_id, function_id)
    elif msg["channel"].startswith("__keyspace@0__:WorkerInfo") and msg["data"] == "hincrby":
      worker_id = msg["channel"].split(":")[2]
      cached_worker_info[worker_id]["export_counter"] += 1
      #print "cached_worker_info is {}".format(cached_worker_info)
    elif msg["channel"] == "ReadyForNewTask":
      worker_id = msg["data"]
      available_workers.append(worker_id)
    else:
      #print "WE DO NOT HANDLE NOTIFICATIONS ON CHANNEL {}".format(msg["channel"])
      # No need to do scheduling in this case.
      continue

    # Schedule things that can be scheduled.
    scheduled_tasks = []
    for task_id in unscheduled_tasks:
      for worker_id in available_workers:
        if can_schedule(worker_id, task_id):
          redis_client.rpush("TaskQueue:Worker{}".format(worker_id), task_id)
          print "Scheduling task {} on worker {}".format(task_id, worker_id)
          scheduled_tasks.append(task_id)
          available_workers.remove(worker_id)
          break
    # Remove the scheduled tasks.
    for task_id in scheduled_tasks:
      unscheduled_tasks.remove(task_id)
