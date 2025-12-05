from Pyro4 import expose
from collections import deque
import random

class Solver:
    def __init__(self, workers=None, input_file_name=None, output_file_name=None):
        self.input_file_name = input_file_name
        self.output_file_name = output_file_name
        self.workers = workers

    def solve(self):
        
        graph_size = self.read_input()
        graph = self.generate_graph(graph_size)
        
        start_vertex = 0
        end_vertex = graph_size - 1
        
        neighbors = graph.get(start_vertex, [])
        
        if not neighbors:
            self.write_output(None, graph_size)
            return
        
        n_workers = len(self.workers)
        
        base_size = len(neighbors) // n_workers
        remainder = len(neighbors) % n_workers
        
        mapped = []
        start_idx = 0
        
        for i in range(n_workers):
            chunk_size = base_size + (1 if i < remainder else 0)
            if chunk_size == 0:
                break
                
            end_idx = start_idx + chunk_size
            worker_neighbors = neighbors[start_idx:end_idx]
            start_idx = end_idx
            
            graph_str = self.serialize_graph(graph)
            
            mapped.append(
                self.workers[i].mymap(
                    graph_str,
                    str(start_vertex),
                    str(end_vertex),
                    ','.join(str(v) for v in worker_neighbors)
                )
            )
        
        shortest_path = self.myreduce(mapped)
        
        self.write_output(shortest_path, graph_size)
    
    @staticmethod
    @expose
    def mymap(graph_str, start_str, end_str, initial_neighbors_str):
        graph = Solver.deserialize_graph(graph_str)
        start = int(start_str)
        end = int(end_str)
        initial_neighbors = [int(v) for v in initial_neighbors_str.split(',') if v]
        
        shortest_path = None
        shortest_length = float('inf')
        
        for initial_vertex in initial_neighbors:
            path = Solver.bfs(graph, start, end, initial_vertex)
            
            if path and len(path) < shortest_length:
                shortest_path = path
                shortest_length = len(path)
        
        return shortest_path
    
    @staticmethod
    def bfs(graph, start, target, first_step):
        queue = deque([(first_step, [start, first_step])])
        visited = {start, first_step}
        
        while queue:
            current, path = queue.popleft()
            
            if current == target:
                return path
            
            for neighbor in graph.get(current, []):
                if neighbor not in visited:
                    visited.add(neighbor)
                    new_path = path + [neighbor]
                    queue.append((neighbor, new_path))
        
        return None
    
    @staticmethod
    @expose
    def myreduce(mapped):
        
        shortest_path = None
        shortest_length = float('inf')
        
        for worker_id, worker_result in enumerate(mapped):
            path = worker_result.value
            
            if path and len(path) < shortest_length:
                shortest_path = path
                shortest_length = len(path)
        
        return shortest_path
    
    def read_input(self):
        f = open(self.input_file_name, 'r')
        graph_size = int(f.readline().strip())
        f.close()
        
        return graph_size
    
    def generate_graph(self, size):
        graph = {i: [] for i in range(size)}
        
        for i in range(size - 1):
            graph[i].append(i + 1)
            if i > 0:
                graph[i].append(i - 1)
        
        if size > 1:
            graph[size - 1].append(size - 2)
        
        for vertex in range(size):
            current_neighbors = len(graph[vertex])
            target_neighbors = random.randint(50, min(100, size - 1))
            
            attempts = 0
            while current_neighbors < target_neighbors and attempts < 100:
                neighbor = random.randint(0, size - 1)
                
                if neighbor != vertex and neighbor not in graph[vertex]:
                    graph[vertex].append(neighbor)
                    if vertex not in graph[neighbor]:
                        graph[neighbor].append(vertex)
                    current_neighbors += 1
                
                attempts += 1
        
        return graph
    
    def write_output(self, path, graph_size):
        f = open(self.output_file_name, 'w')
        
        f.write("Graph size: %d vertices\n" % graph_size)
        f.write("Search: vertex 0 -> vertex %d\n\n" % (graph_size - 1))
        
        if path:
            f.write("Shortest path found:\n")
            f.write(" -> ".join(str(v) for v in path) + "\n\n")
            f.write("Path length: %d vertices\n" % len(path))
            f.write("Number of edges: %d\n" % (len(path) - 1))
        else:
            f.write("No path exists between vertices\n")
        
        f.close()
        print("Output written")
    
    @staticmethod
    def serialize_graph(graph):
        parts = []
        for vertex in sorted(graph.keys()):
            neighbors = graph[vertex]
            neighbor_str = ",".join(str(n) for n in neighbors)
            parts.append("%d:%s" % (vertex, neighbor_str))
        return ";".join(parts)
    
    @staticmethod
    def deserialize_graph(graph_str):
        graph = {}
        if not graph_str:
            return graph
        
        for part in graph_str.split(";"):
            if ":" in part:
                vertex_str, neighbors_str = part.split(":", 1)
                vertex = int(vertex_str)
                neighbors = [int(n) for n in neighbors_str.split(",") if n]
                graph[vertex] = neighbors
        
        return graph