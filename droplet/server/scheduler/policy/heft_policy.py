#  Copyright 2019 U.C. Berkeley RISE Lab
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import logging
import random
import time

import zmq

from droplet.shared.proto.droplet_pb2 import GenericResponse
from droplet.shared.proto.metadata_pb2 import KeySizeData # TODO: make sure this works
from droplet.shared.proto.shared_pb2 import StringSet
from droplet.server.scheduler.policy.base_policy import (
    BaseDropletSchedulerPolicy
)
from droplet.server.scheduler.predictor.cost_predictor import *
from droplet.server.scheduler.utils import (
    get_cache_ip_key,
    get_pin_address,
    get_unpin_address
)
import droplet.server.utils as sutils

sys_random = random.SystemRandom()


class HeftDropletSchedulerPolicy(BaseDropletSchedulerPolicy):

    def __init__(self, pin_accept_socket, pusher_cache, kvs_client, ip,
                 random_threshold=0.20):
        # This scheduler's IP address.
        self.ip = ip

        # A socket to listen for confirmations of pin operations' successes.
        self.pin_accept_socket = pin_accept_socket

        # A cache for zmq.PUSH sockets.
        self.pusher_cache = pusher_cache

        # This thread's Anna KVS client.
        self.kvs_client = kvs_client

        # A map to track how many requests have been routed to each executor in
        # the most recent timeslice.
        self.running_counts = {}

        # A map to track nodes which have recently reported high load. These
        # nodes will not be sent requests until after a cooling period.
        self.backoff = {}

        # A map to track which caches are currently caching which keys.
        self.key_locations = {}

        # Executors which currently have no functions pinned on them.
        self.unpinned_executors = set()

        # A map from function names to the executor(s) on which they are
        # pinned.
        self.function_locations = {}

        # A map to sequester function location information until all functions
        # in a DAG have accepted their pin operations.
        self.pending_dags = {}

        # The most recently reported statuses of each executor thread.
        self.thread_statuses = {}

        # This quantifies how many requests should be routed stochastically
        # rather than by policy.
        self.random_threshold = random_threshold

        # Cost Prediction Models
        self.runtime_predictors = CostPredictors()
        self.output_predictors = CostPredictors()

        # Key Size Map
        self.keySizes = dict()

        # Counter for running regression models
        self.counter = 1


    def reverse_top_sort_util(self, curr, visited, stack, dag, fnames):
        visited[curr] = True
        for f in sutils.get_dag_predecessors(dag, fnames[curr]):
            ind = fnames.index(f)
            if visited[ind] == False:
                self.reverse_top_sort_util(ind, visited, stack, dag, fnames)

        stack.insert(0, fnames[curr])

    def top_sort_util(self, curr, visited, stack, dag, fnames):
        visited[curr] = True
        for f in self.get_dag_successors(dag, fnames[curr]):
            ind = fnames.index(f)
            if visited[ind] == False:
                self.top_sort_util(ind, visited, stack, dag, fnames)

        stack.insert(0, fnames[curr])

    def top_sort(self, dag, fnames, reverse=False):
        print('Fnames: ', fnames)
        visited = [False] * len(fnames)
        stack = []
        for i in range(len(fnames)):
            if visited[i] == False:
                if reverse:
                    self.reverse_top_sort_util(i, visited, stack, dag, fnames)
                else:
                    self.top_sort_util(i, visited, stack, dag, fnames)

        return stack

    def get_dag_successors(self, dag, fname):
        result = []
        for connection in dag.connections:
            if connection.source == fname:
                result.append(connection.sink)

        return result

    def rank_upward(self, fnames, dag, runtime_predictions, reverse_top_sorted):
        ranks = {}

        for f in reverse_top_sorted:
            successors = self.get_dag_successors(dag, f)
            if not successors:
                ranks[f] = runtime_predictions[f]
            else:
                max_successor_rank = float('-inf')
                for successor in successors:
                    if ranks[successor] > max_successor_rank:
                        max_successor_rank = ranks[successor]

                ranks[f] = runtime_predictions[f] + max_successor_rank

        return ranks

    def predict_costs(self, functions, dag, top_sorted):
        runtime_predictions = {}
        output_predictions = {}

        for f in top_sorted:
            predecessors = sutils.get_dag_predecessors(dag, f)
            input_size = 0
            for ref in functions[f]:
                input_size += self.keySizes[ref.key]
            if predecessors:
                for pred in predecessors:
                    input_size += output_predictions[pred]

            runtime_predictions[f] = self.runtime_predictors.compute_cost(f, input_size)
            output_predictions[f] = self.output_predictors.compute_cost(f, input_size)

        return runtime_predictions

    # Functions is a map {fname --> refs}
    def pick_executors(self, functions, dag):
        if self.counter % 5 == 0:
            self.update_predictor_models()
        self.counter += 1
        # If any of the computation costs are unknown, 
        # defer to the default policy
        for fname in functions:
            if not self.runtime_predictors.has_predictor(fname) or not self.output_predictors.has_predictor(fname):
                mapping = {}
                for fname, ref in functions.items():
                    mapping[fname] = self.pick_executor(ref, fname)
                return mapping

        # Perform topological sort
        topSort = self.top_sort(dag, list(functions.keys()))

        # Predict runtimes and output sizes
        runtime_predictions = self.predict_costs(functions, dag, topSort)

        # Compute the upward rank of functions with known computation cost
        ranks = self.rank_upward(list(functions.keys()), dag, runtime_predictions, topSort.reverse())
        rank_keys = sorted(ranks, key=lambda k: ranks[k], reverse=True)

        # Assign functions to executors that minimize Earliest Finish Time
        scheduled = {}
        mapping = {}
        actual_finish_time = {}
        while len(rank_keys) > 0:
            f = rank_keys.pop(0)
            executors = set(e for e in self.function_locations[f])
            min_eft = float('inf')
            best_executor = None
            
            # Compute EFT
            for e in executors:
                est = None
                if not sutils.get_dag_predecessors(dag, f):
                    est = 0
                else:
                    avail = actual_finish_time[scheduled[e][-1]] if e in scheduled else 0
                    predAFT = 0
                    for pred in sutils.get_dag_predecessors(dag, f):
                        predAFT = max(predAFT, actual_finish_time[pred])
                    est = max(avail, predAFT)
                eft = runtime_predictions[f] + est
                if eft < min_eft:
                    min_eft = eft
                    best_executor = e
            if e not in scheduled:
                scheduled[e] = []
            scheduled[e].append(f)
            mapping[f] = e
            actual_finish_time[f] = min_eft
        return mapping

    def pick_executor(self, references, function_name=None):
        # Construct a map which maps from IP addresses to the number of
        # relevant arguments they have cached. For the time begin, we will
        # just pick the machine that has the most number of keys cached.
        arg_map = {}

        if function_name:
            executors = set(self.function_locations[function_name])
        else:
            executors = set(self.unpinned_executors)

        for executor in self.backoff:
            executors.discard(executor)

        # Generate a list of all the keys in the system; if any of these nodes
        # have received many requests, we remove them from the executor set
        # with high probability.
        for key in self.running_counts:
            if (len(self.running_counts[key]) > 1000 and sys_random.random() >
                    self.random_threshold):
                executors.discard(key)

        if len(executors) == 0:
            return None

        executor_ips = set([e[0] for e in executors])

        # For each reference, we look at all the places where they are cached,
        # and we calculate which IP address has the most references cached.
        for reference in references:
            if reference.key in self.key_locations:
                ips = self.key_locations[reference.key]

                for ip in ips:
                    # Only choose this cached node if its a valid executor for
                    # our purposes.
                    if ip in executor_ips:
                        if ip not in arg_map:
                            arg_map[ip] = 0

                        arg_map[ip] += 1

        # Get the IP address that has the maximum value in the arg_map, if
        # there are any values.
        max_ip = None
        if arg_map:
            max_ip = max(arg_map, key=arg_map.get)

        # Pick a random thead from our potential executors that is on that IP
        # address with the most keys cached.
        if max_ip:
            candidates = list(filter(lambda e: e[0] == max_ip, executors))
            max_ip = sys_random.choice(candidates)

        # If max_ip was never set (i.e. there were no references cached
        # anywhere), or with some random chance, we assign this node to a
        # random executor.
        if not max_ip or sys_random.random() < self.random_threshold:
            max_ip = sys_random.sample(executors, 1)[0]

        if max_ip not in self.running_counts:
            self.running_counts[max_ip] = set()

        self.running_counts[max_ip].add(time.time())

        # Remove this IP/tid pair from the system's metadata until it notifies
        # us that it is available again, but only do this for non-DAG requests.
        if not function_name:
            self.unpinned_executors.discard(max_ip)

        return max_ip

    def pin_function(self, dag_name, function_name):
        # If there are no functions left to choose from, then we return None,
        # indicating that we ran out of resources to use.
        if len(self.unpinned_executors) == 0:
            return False

        if dag_name not in self.pending_dags:
            self.pending_dags[dag_name] = []

        # Make a copy of the set of executors, so that we don't modify the
        # system's metadata.
        candidates = set(self.unpinned_executors)

        while True:
            # Pick a random executor from the set of candidates and attempt to
            # pin this function there.
            node, tid = sys_random.sample(candidates, 1)[0]

            sckt = self.pusher_cache.get(get_pin_address(node, tid))
            msg = self.ip + ':' + function_name
            sckt.send_string(msg)

            response = GenericResponse()
            try:
                response.ParseFromString(self.pin_accept_socket.recv())
            except zmq.ZMQError:
                logging.error('Pin operation to %s:%d timed out. Retrying.' %
                              (node, tid))
                continue

            # Do not use this executor either way: If it rejected, it has
            # something else pinned, and if it accepted, it has pinned what we
            # just asked it to pin.
            # self.unpinned_executors.discard((node, tid))
            candidates.discard((node, tid))

            if response.success:
                # The pin operation succeeded, so we return the node and thread
                # ID to the caller.
                self.pending_dags[dag_name].append((function_name, (node,
                                                                    tid)))
                return True
            else:
                # The pin operation was rejected, remove node and try again.
                logging.error('Node %s:%d rejected pin for %s. Retrying.'
                              % (node, tid, function_name))

                continue

    def commit_dag(self, dag_name):
        for function_name, location in self.pending_dags[dag_name]:
            if function_name not in self.function_locations:
                self.function_locations[function_name] = set()

            self.function_locations[function_name].add(location)

        del self.pending_dags[dag_name]

    def discard_dag(self, dag, pending=False):
        if pending:
            # If the DAG was pending, we can simply look at the sequestered
            # pending metadata.
            pinned_locations = list(self.pending_dags[dag.name])
            del self.pending_dags[dag.name]
        else:
            # If the DAG was not pinned, we construct a set of all the
            # locations where functions were pinned for this DAG.
            pinned_locations = []
            for function_name in dag.functions:
                for location in self.function_locations[function_name]:
                    pinned_locations.append((function_name, location))

        # For each location, we fire-and-forget an unpin message.
        for function_name, location in pinned_locations:
            ip, tid = location

            sckt = self.pusher_cache.get(get_unpin_address(ip, tid))
            sckt.send_string(function_name)

    def process_status(self, status):
        key = (status.ip, status.tid)
        logging.info('Received status update from executor %s:%d.' %
                     (key[0], int(key[1])))

        # This means that this node is currently departing, so we remove it
        # from all of our metadata tracking.
        if not status.running:
            if key in self.thread_statuses:
                for fname in self.thread_statuses[key].functions:
                    self.function_locations[fname].discard(key)

                del self.thread_statuses[key]

            self.unpinned_executors.discard(key)
            return

        if len(status.functions) == 0:
            self.unpinned_executors.add(key)

        # Remove all the old function locations, and all the new ones -- there
        # will probably be a large overlap, but this shouldn't be much
        # different than calculating two different set differences anyway.
        if key in self.thread_statuses and self.thread_statuses[key] != status:
            for function_name in self.thread_statuses[key].functions:
                if function_name in self.function_locations:
                    self.function_locations[function_name].discard(key)

        self.thread_statuses[key] = status
        for function_name in status.functions:
            if function_name not in self.function_locations:
                self.function_locations[function_name] = set()

            self.function_locations[function_name].add(key)

        # If the executor thread is overutilized, we add it to the backoff set
        # and ignore it for a period of time.
        if status.utilization > 0.70:
            self.backoff[key] = time.time()

    def update(self):
        # Periodically clean up the running counts map to drop any times older
        # than 5 seconds.
        for executor in self.running_counts:
            new_set = set()
            for ts in self.running_counts[executor]:
                if time.time() - ts < 5:
                    new_set.add(ts)

            self.running_counts[executor] = new_set

        # Clean up any backoff messages that were added more than 5 seconds ago
        # -- this should be enough to drain a queue.
        remove_set = set()
        for executor in self.backoff:
            if time.time() - self.backoff[executor] > 5:
                remove_set.add(executor)

        for executor in remove_set:
            del self.backoff[executor]

        executors = set(map(lambda status: status.ip,
                            self.thread_statuses.values()))

        # Update the sets of keys that are being cached at each IP address.
        self.key_locations.clear()
        for ip in executors:
            key = get_cache_ip_key(ip)

            # This is of type LWWPairLattice, which has a StringSet protobuf
            # packed into it; we want the keys in that StringSet protobuf.
            lattice = self.kvs_client.get(key)[key]
            if lattice is None:
                # We will only get None if this executor is still joining; if
                # so, we just ignore this for now and move on.
                continue

            st = StringSet()
            st.ParseFromString(lattice.reveal())

            for key in st.keys:
                if key not in self.key_locations:
                    self.key_locations[key] = []

                self.key_locations[key].append(ip)
        self.update_key_sizes()

    def update_function_locations(self, new_locations):
        for location in new_locations:
            function_name = location.name
            if function_name not in self.function_locations:
                self.function_locations[function_name] = set()

            key = (location.ip, location.tid)
            self.function_locations[function_name].add(key)

    def update_predictor_data(self, fname, inputRefs, inputSizes, outputSize, runtime):
        total_input_size = 0 
        for ref in inputRefs:
            if ref not in self.keySizes:
                self.update_key_sizes()
            print(self.keySizes)
            total_input_size += self.keySizes[ref]

        total_input_size += sum(inputSizes)

        if not self.runtime_predictors.has_predictor(fname):
            self.runtime_predictors.add_function(fname)            
        if not self.output_predictors.has_predictor(fname):
            self.output_predictors.add_function(fname)
        
        self.runtime_predictors.add_new_result(fname, total_input_size, runtime)
        self.output_predictors.add_new_result(fname, total_input_size, outputSize)

    def update_predictor_models(self):
        self.runtime_predictors.update_all_models()
        self.output_predictors.update_all_models()

    def update_key_sizes(self):
        # TODO: Figure out how to make this work for all nodes
        s = 'ANNA_METADATA|size|52.207.241.26|172.20.39.144|0|MEMORY'
        try:
            lat = self.kvs_client.get(s)[s]
        except:
            break
        if not lat:
            break

        # TODO: Make sure this is imported correctly (check above)
        ksd = KeySizeData()
        ksd.ParseFromString(lat.val)

        key_sizes = ksd.key_sizes

        for k in key_sizes:
            key = k.key
            size = k.size
            self.keySizes[key] = size
