import io
from typing import List, Optional


class GraphKeyWritable:
    """
    Python equivalent of the Java GraphKeyWritable class.
    Assumes WritableComparable interface and Assert functionality 
    are implemented in other modules.
    """
    
    def __init__(self):
        self.parts: Optional[List[int]] = None
        self.no_component: int = 0
    
    def set(self, graph_key: str) -> None:
        """
        Set the graph key from a comma-separated string.
        
        Args:
            graph_key: String in format "i,j" or "i,j,k"
        """
        p = graph_key.split(",")
        self.no_component = len(p)
        
        # Only G_{ijk} or G_{ij} are acceptable!!!
        assert self.no_component == 2 or self.no_component == 3, \
            "Only G_{ijk} or G_{ij} are acceptable"
        
        self.parts = [0] * len(p)
        pre = -1
        
        for i in range(len(p)):
            self.parts[i] = int(p[i])
            
            if pre == -1:
                pre = self.parts[i]
            else:
                assert self.parts[i] > pre, "Must be increasing!!"
                pre = self.parts[i]
    
    def read_fields(self, data_input) -> None:
        """
        Read fields from a data input stream.
        Assumes data_input has read_int() method.
        """
        self.no_component = data_input.read_int()
        self.parts = [0] * self.no_component
        
        for i in range(self.no_component):
            self.parts[i] = data_input.read_int()
    
    def write(self, data_output) -> None:
        """
        Write fields to a data output stream.
        Assumes data_output has write_int() method.
        """
        data_output.write_int(self.no_component)
        
        for i in range(self.no_component):
            data_output.write_int(self.parts[i])
    
    def compare_to(self, target: 'GraphKeyWritable') -> int:
        """
        Compare this object to another GraphKeyWritable.
        
        Args:
            target: Another GraphKeyWritable instance
            
        Returns:
            int: Comparison result (-1, 0, 1)
        """
        self_str = str(self).lower()
        target_str = str(target).lower()
        
        if self_str < target_str:
            return -1
        elif self_str > target_str:
            return 1
        else:
            return 0
    
    def __str__(self) -> str:
        """
        String representation of the object.
        
        Returns:
            str: Space-separated string of parts
        """
        if self.parts is None or self.no_component == 0:
            return ""
        
        parts_str = []
        for i in range(self.no_component - 1):
            parts_str.append(str(self.parts[i]))
        
        parts_str.append(str(self.parts[self.no_component - 1]))
        return " ".join(parts_str)
    
    def __hash__(self) -> int:
        """
        Hash code for the object.
        
        Returns:
            int: Hash value based on string representation
        """
        return hash(str(self))
    
    def __eq__(self, other) -> bool:
        """
        Equality comparison.
        
        Args:
            other: Another object to compare with
            
        Returns:
            bool: True if objects are equal
        """
        if not isinstance(other, GraphKeyWritable):
            return False
        return str(self) == str(other)
    
    def __lt__(self, other) -> bool:
        """
        Less than comparison for sorting.
        
        Args:
            other: Another GraphKeyWritable instance
            
        Returns:
            bool: True if this object is less than other
        """
        return self.compare_to(other) < 0