class TripleWritable:
    PRIME = 1000003

    def __init__(self, left=None, mid=None, right=None):
        self.left = left
        self.mid = mid
        self.right = right

    def set_from_string(self, subgraph_key: str):
        try:
            parts = subgraph_key.split()
            self.left = int(parts[0])
            self.mid = int(parts[1])
            self.right = int(parts[2])
            assert self.right > self.mid > self.left, "Wrong subgraph"
        except Exception as e:
            print(f"Error parsing subgraph_key '{subgraph_key}': {e}")

    def set(self, a: int, b: int, c: int):
        # Ordina in modo che left < mid < right
        nums = sorted([a, b, c])
        self.left, self.mid, self.right = nums
        assert self.right > self.mid > self.left, "Wrong subgraph"

    def read_fields(self, data_input):
        # data_input deve supportare read(4) come file-like object
        self.left = int.from_bytes(data_input.read(4), 'big')
        self.mid = int.from_bytes(data_input.read(4), 'big')
        self.right = int.from_bytes(data_input.read(4), 'big')

    def write(self, data_output):
        # data_output deve supportare write(bytes)
        data_output.write(self.left.to_bytes(4, 'big'))
        data_output.write(self.mid.to_bytes(4, 'big'))
        data_output.write(self.right.to_bytes(4, 'big'))

    def __lt__(self, other):
        if not isinstance(other, TripleWritable):
            return NotImplemented
        if self.left != other.left:
            return self.left < other.left
        if self.mid != other.mid:
            return self.mid < other.mid
        return self.right < other.right

    def __eq__(self, other):
        if not isinstance(other, TripleWritable):
            return False
        return (self.left == other.left and
                self.mid == other.mid and
                self.right == other.right)

    def __hash__(self):
        return self.left * self.PRIME * self.PRIME + self.mid * self.PRIME + self.right * self.PRIME

    def __str__(self):
        return f"{self.left} {self.mid} {self.right}"
