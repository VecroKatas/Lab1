# -*- coding: utf-8 -*-
from __future__ import division
import time
import random
from Pyro4 import expose

try:
    xrange
except NameError:
    xrange = range

@expose
class Solver:
    """
    Паралельний пошук всіх шляхів між двома вершинами у графі.
    Використовує ІТЕРАТИВНИЙ DFS для уникнення переповнення стеку.
    """
    def __init__(self, workers=None, input_file_name=None, output_file_name=None):
        self.workers = workers
        self.input_file_name = input_file_name
        self.output_file_name = output_file_name
        print "GraphPathFinder initialized"

    def solve(self):
        print "=== Parallel Path Finding Started ==="
        start_time = time.time()
        
        n = self.read_input_size()
        graph = self.generate_random_graph(n)
        
        workers_to_use = self.workers if self.workers is not None else []
        num_workers = len(workers_to_use)
        
        print "Graph size: %d vertices" % n
        print "Workers: %d" % num_workers
        
        edge_count = sum(len(neighbors) for neighbors in graph.values())
        print "Edges: %d" % edge_count
        
        start_vertex = 0
        target_vertex = n - 1
        
        print "Finding paths: %d -> %d" % (start_vertex, target_vertex)
        
        all_paths = []
        
        # Паралельний пошук
        if num_workers > 0 and start_vertex in graph and len(graph[start_vertex]) > 0:
            start_neighbors = graph[start_vertex]
            print "Start neighbors: %d neighbors" % len(start_neighbors)
            
            chunk_size = max(1, len(start_neighbors) // num_workers)
            tasks = []
            
            for w_idx in xrange(num_workers):
                if w_idx >= len(workers_to_use):
                    break
                    
                start_idx = w_idx * chunk_size
                if w_idx == num_workers - 1:
                    end_idx = len(start_neighbors)
                else:
                    end_idx = start_idx + chunk_size
                
                if start_idx < len(start_neighbors):
                    neighbor_chunk = start_neighbors[start_idx:end_idx]
                    worker = workers_to_use[w_idx]
                    
                    print "Worker %d: %d neighbors" % (w_idx + 1, len(neighbor_chunk))
                    
                    task = worker.find_paths_worker_iterative(
                        graph,
                        start_vertex,
                        target_vertex,
                        neighbor_chunk,
                        min(n * 2, 750)  # Обмеження довжини шляху
                    )
                    tasks.append(task)
            
            for task_idx, task in enumerate(tasks):
                worker_paths = task.value
                if worker_paths:
                    print "Worker %d: %d paths" % (task_idx + 1, len(worker_paths))
                    all_paths.extend(worker_paths)
        else:
            print "Sequential mode"
            all_paths = self.find_all_paths_iterative(graph, start_vertex, target_vertex, min(n * 2, 750))
        
        elapsed = time.time() - start_time
        
        # Видалення дублікатів
        unique_paths = []
        seen = set()
        for path in all_paths:
            path_tuple = tuple(path)
            if path_tuple not in seen:
                seen.add(path_tuple)
                unique_paths.append(path)
        
        all_paths = unique_paths
        
        self.write_output(all_paths, elapsed, n, start_vertex, target_vertex, num_workers)
        
        print "=== Completed in %.4f sec ===" % elapsed
        print "Total paths: %d" % len(all_paths)
        
        return all_paths

    def read_input_size(self):
        """Читає розмір графа з файлу"""
        try:
            with open(self.input_file_name, 'r') as f:
                lines = [line.strip() for line in f if line.strip()]
            n = int(lines[0])
            if n < 2:
                raise ValueError("N must be >= 2")
            return n
        except Exception as e:
            print "Error reading input: %s" % str(e)
            return 15

    @staticmethod
    def generate_random_graph(n, density=0.02):
        """
        Генерує випадковий граф з МЕНШОЮ щільністю для великих N.
        Для N=1000 використовуємо density=0.15 замість 0.3
        """
        graph = {i: [] for i in xrange(n)}
        random.seed(42)
        
        # Базове дерево для зв'язності
        for i in xrange(1, n):
            parent = random.randint(0, i - 1)
            graph[parent].append(i)
        
        # Додаткові ребра (менше для великих графів)
        max_edges = n * (n - 1) // 2
        current_edges = n - 1
        target_edges = int(max_edges * density)
        
        attempts = 0
        max_attempts = min(target_edges * 10, n * 100)
        
        while current_edges < target_edges and attempts < max_attempts:
            i = random.randint(0, n - 1)
            j = random.randint(0, n - 1)
            
            if i != j and j not in graph[i]:
                graph[i].append(j)
                current_edges += 1
            attempts += 1
        
        for vertex in graph:
            graph[vertex].sort()
        
        return graph

    @staticmethod
    @expose
    def find_paths_worker_iterative(graph, start_vertex, target_vertex, start_neighbors, max_length):
        """
        ІТЕРАТИВНИЙ DFS для уникнення переповнення стеку рекурсії.
        Використовує явний стек замість рекурсії.
        """
        all_paths = []
        max_paths = 10000  # Обмеження на кількість шляхів від одного воркера
        
        for first_neighbor in start_neighbors:
            if len(all_paths) >= max_paths:
                break
            
            # Стек зберігає: (поточна_вершина, шлях, множина_відвіданих)
            stack = [(first_neighbor, [start_vertex, first_neighbor], {start_vertex, first_neighbor})]
            
            while stack and len(all_paths) < max_paths:
                current, path, visited = stack.pop()
                
                # Перевірка довжини шляху
                if len(path) > max_length:
                    continue
                
                # Знайшли ціль
                if current == target_vertex:
                    all_paths.append(list(path))
                    continue
                
                # Додаємо сусідів до стеку
                if current in graph:
                    # Обробляємо сусідів у зворотному порядку для правильного порядку обходу
                    for neighbor in reversed(graph[current]):
                        if neighbor not in visited:
                            new_visited = visited | {neighbor}
                            new_path = path + [neighbor]
                            stack.append((neighbor, new_path, new_visited))
        
        return all_paths

    def find_all_paths_iterative(self, graph, start, target, max_length):
        """
        Послідовний ІТЕРАТИВНИЙ пошук всіх шляхів.
        """
        all_paths = []
        max_paths = 5000
        
        stack = [(start, [start], {start})]
        
        while stack and len(all_paths) < max_paths:
            current, path, visited = stack.pop()
            
            if len(path) > max_length:
                continue
            
            if current == target:
                all_paths.append(list(path))
                continue
            
            if current in graph:
                for neighbor in reversed(graph[current]):
                    if neighbor not in visited:
                        new_visited = visited | {neighbor}
                        new_path = path + [neighbor]
                        stack.append((neighbor, new_path, new_visited))
        
        return all_paths

    def write_output(self, paths, execution_time, n, start, target, num_workers):
        """Запис результатів"""
        with open(self.output_file_name, 'w') as f:
            f.write("=== Path Finding Results ===\n")
            f.write("Graph size: %d vertices\n" % n)
            f.write("Search: %d -> %d\n" % (start, target))
            f.write("Workers: %d\n" % num_workers)
            f.write("Total paths: %d\n" % len(paths))
            f.write("Time: %.4f sec\n" % execution_time)
            f.write("\n")
            
            if len(paths) > 0:
                lengths = [len(p) - 1 for p in paths]
                f.write("=== Statistics ===\n")
                f.write("Shortest: %d edges\n" % min(lengths))
                f.write("Longest: %d edges\n" % max(lengths))
                f.write("Average: %.2f edges\n" % (sum(lengths) / len(lengths)))
                f.write("\n")
                
                f.write("=== First 10 Paths ===\n")
                for i, path in enumerate(paths[:10]):
                    if len(path) <= 20:
                        path_str = " -> ".join(str(v) for v in path)
                    else:
                        path_str = " -> ".join(str(v) for v in path[:10]) + " ... " + str(path[-1])
                    f.write("%d. %s (len: %d)\n" % (i + 1, path_str, len(path) - 1))
                
                if len(paths) > 10:
                    f.write("\n... and %d more\n" % (len(paths) - 10))
            else:
                f.write("No paths found\n")