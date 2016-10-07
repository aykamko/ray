import os
import sys
import time
import subprocess32 as subprocess
import string
import random

# Ray modules
import config

_services_env = os.environ.copy()
_services_env["PATH"] = os.pathsep.join([os.path.dirname(os.path.abspath(__file__)), _services_env["PATH"]])
# Make GRPC only print error messages.
_services_env["GRPC_VERBOSITY"] = "ERROR"

# all_processes is a list of the scheduler, object store, and worker processes
# that have been started by this services module if Ray is being used in local
# mode.
all_processes = []

TIMEOUT_SECONDS = 5

def address(host, port):
  return host + ":" + str(port)

def new_redis_port():
  return random.randint(10000, 65535)

def new_scheduler_port():
  return random.randint(10000, 65535)

def cleanup():
  """When running in local mode, shutdown the Ray processes.

  This method is used to shutdown processes that were started with
  services.start_ray_local(). It kills all scheduler, object store, and worker
  processes that were started by this services module. Driver processes are
  started and disconnected by worker.py.
  """
  global all_processes
  successfully_shut_down = True
  # Terminate the processes in reverse order.
  for p in all_processes[::-1]:
    if p.poll() is not None: # process has already terminated
      continue
    p.kill()
    time.sleep(0.05) # is this necessary?
    if p.poll() is not None:
      continue
    p.terminate()
    time.sleep(0.05) # is this necessary?
    if p.poll is not None:
      continue
    successfully_shut_down = False
  if successfully_shut_down:
    print "Successfully shut down Ray."
  else:
    print "Ray did not shut down properly."
  all_processes = []

def start_redis(port):
  redis_filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../../thirdparty/photon/common/thirdparty/redis-3.2.3/src/redis-server")
  p = subprocess.Popen([redis_filepath, "--port", str(port), "--loglevel", "warning"])
  if cleanup:
    all_processes.append(p)

def start_local_scheduler(redis_address):
  local_scheduler_filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../../thirdparty/photon/build/photon_scheduler")
  local_scheduler_name = "/tmp/scheduler{}".format(random.randint(0, 10000))
  p = subprocess.Popen([local_scheduler_filepath, "-s", local_scheduler_name, "-r", redis_address])
  if cleanup:
    all_processes.append(p)
  return local_scheduler_name

def start_scheduler(redis_address, cleanup):
  """This method starts a scheduler process.

  Args:
    cleanup (bool): True if using Ray in local mode. If cleanup is true, then
      this process will be killed by serices.cleanup() when the Python process
      that imported services exits.
  """
  scheduler_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../../lib/python/ray/scheduler.py")
  p = subprocess.Popen(["python", scheduler_path, "--redis-address", redis_address])

  #scheduler_port = scheduler_address.split(":")[1]
  #p = subprocess.Popen(["scheduler", scheduler_address, "--log-file-name", config.get_log_file_path("scheduler-" + scheduler_port + ".log")], env=_services_env)
  if cleanup:
    all_processes.append(p)

def start_objstore(node_ip_address, cleanup):
  """This method starts an object store process.

  Args:
    node_ip_address (str): The ip address of the node running the object store.
    cleanup (bool): True if using Ray in local mode. If cleanup is true, then
      this process will be killed by serices.cleanup() when the Python process
      that imported services exits.
  """
  plasma_store_executable = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../../../thirdparty/plasma/build/plasma_store")
  store_name = "/tmp/ray_plasma_store{}".format(random.randint(0, 10000))
  p1 = subprocess.Popen([plasma_store_executable, "-s", store_name])

  plasma_manager_executable = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../../../thirdparty/plasma/build/plasma_manager")
  manager_port = random.randint(10000, 50000)
  p2 = subprocess.Popen([plasma_manager_executable, "-s", store_name, "-m", node_ip_address, "-p", str(manager_port)])

  if cleanup:
    all_processes.append(p1)
    all_processes.append(p2)

  return store_name, manager_port

def start_worker(node_ip_address, redis_address, object_store_name, object_store_manager_port, local_scheduler_name, worker_path, objstore_address=None, cleanup=True):
  """This method starts a worker process.

  Args:
    node_ip_address (str): The IP address of the node that the worker runs on.
    redis_address (str): TODO
    worker_path (str): The path of the source code which the worker process will
      run.
    objstore_address (Optional[str]): The ip address and port of the object
      store to connect to.
    cleanup (Optional[bool]): True if using Ray in local mode. If cleanup is
      true, then this process will be killed by serices.cleanup() when the
      Python process that imported services exits. This is True by default.
  """
  command = ["python",
             worker_path,
             "--node-ip-address=" + node_ip_address,
             "--object-store-name=" + object_store_name,
             "--object-store-manager-port=" + str(object_store_manager_port),
             "--local-scheduler-name=" + local_scheduler_name,
             "--redis-address=" + redis_address]
  if objstore_address is not None:
    command.append("--objstore-address=" + objstore_address)
  p = subprocess.Popen(command)
  if cleanup:
    all_processes.append(p)

def start_node(redis_address, node_ip_address, num_workers, worker_path=None, cleanup=False):
  """Start an object store and associated workers in the cluster setting.

  This starts an object store and the associated workers when Ray is being used
  in the cluster setting. This assumes the scheduler has already been started.

  Args:
    redis_address (str): TODO
    node_ip_address (str): IP address (without port) of the node this function
      is run on.
    num_workers (int): The number of workers to be started on this node.
    worker_path (str): Path of the Python worker script that will be run on the
      worker.
    cleanup (bool): If cleanup is True, then the processes started by this
      command will be killed when the process that imported services exits.
  """
  start_objstore(node_ip_address, cleanup=cleanup)
  time.sleep(0.2)
  if worker_path is None:
    worker_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../../scripts/default_worker.py")
  for _ in range(num_workers):
    start_worker(node_ip_address, redis_address, worker_path, cleanup=cleanup)
  time.sleep(0.5)

def start_ray_local(node_ip_address="127.0.0.1", num_objstores=1, num_workers=0, worker_path=None):
  """Start Ray in local mode.

  This method starts Ray in local mode (as opposed to cluster mode, which is
  handled by cluster.py).

  Args:
    num_objstores (int): The number of object stores to start. Aside from
      testing, this should be one.
    num_workers (int): The number of workers to start.
    worker_path (str): The path of the source code that will be run by the
      worker.

  Returns:
    The address of the scheduler and the addresses of all of the object stores.
  """
  if worker_path is None:
    worker_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../../scripts/default_worker.py")
  if num_objstores < 1:
    raise Exception("`num_objstores` is {}, but should be at least 1.".format(num_objstores))
  redis_port = new_redis_port()
  redis_address = address(node_ip_address, redis_port)
  start_redis(redis_port)
  time.sleep(0.1)

  local_scheduler_name = start_local_scheduler(redis_address)
  time.sleep(0.1)

  #scheduler_address = address(node_ip_address, new_scheduler_port())
  start_scheduler(redis_address, cleanup=True)
  time.sleep(0.1)
  # create objstores
  object_store_info = []
  for i in range(num_objstores):
    object_store_name, object_store_manager_port = start_objstore(node_ip_address, cleanup=True)
    object_store_info.append((object_store_name, object_store_manager_port))
    time.sleep(0.2)
    if i < num_objstores - 1:
      num_workers_to_start = num_workers / num_objstores
    else:
      # In case num_workers is not divisible by num_objstores, start the correct
      # remaining number of workers.
      num_workers_to_start = num_workers - (num_objstores - 1) * (num_workers / num_objstores)
    for _ in range(num_workers_to_start):
      start_worker(node_ip_address, redis_address, object_store_name, object_store_manager_port, local_scheduler_name, worker_path, cleanup=True)
    time.sleep(0.3)

  return redis_address, object_store_info, local_scheduler_name
