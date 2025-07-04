import struct
from io import BytesIO
from bitarray import bitarray
from writable.Settings import Settings


class SpecialEdgeValueWritable:
    """
    Traduzione Python della classe Java SpecialEdgeValueWritable
    Implementa la serializzazione/deserializzazione compatibile con Hadoop Writable
    """
    
    def __init__(self):
        self.type = None  # char in Java -> string di 1 carattere in Python
        self.value = 0.0  # double
        self.no_bits = 0  # int
        self.sliding = None  # BitSet -> bitarray in Python
    
    def init(self, type_char, value, no_bits, sliding):
        """
        Inizializza l'oggetto con i parametri forniti
        
        Args:
            type_char: carattere che rappresenta il tipo
            value: valore double
            no_bits: numero di bits
            sliding: oggetto BitSet/bitarray
        """
        self.type = type_char
        self.value = value
        self.sliding = sliding
        self.no_bits = no_bits
    
    def read_fields(self, data_input):
        """
        Legge i campi da un DataInput (equivalente al metodo readFields di Java)
        
        Args:
            data_input: oggetto simile a DataInput (es. BytesIO)
        """
        # Legge il carattere (2 bytes per char in Java)
        char_bytes = data_input.read(2)
        if len(char_bytes) < 2:
            raise IOError("Impossibile leggere il carattere")
        self.type = struct.unpack('>H', char_bytes)[0]  # big-endian unsigned short
        self.type = chr(self.type)  # converte in carattere
        
        if self.type == Settings.EDGE_TYPE:
            # Legge double (8 bytes)
            double_bytes = data_input.read(8)
            if len(double_bytes) < 8:
                raise IOError("Impossibile leggere il valore double")
            self.value = struct.unpack('>d', double_bytes)[0]
            
        elif self.type == Settings.STAR_GRAPH:
            raise RuntimeError("Something wrong with this")
            
        elif self.type == Settings.D_TYPE:
            double_bytes = data_input.read(8)
            if len(double_bytes) < 8:
                raise IOError("Impossibile leggere il valore double")
            self.value = struct.unpack('>d', double_bytes)[0]
            
        elif self.type == Settings.C_TYPE:
            double_bytes = data_input.read(8)
            if len(double_bytes) < 8:
                raise IOError("Impossibile leggere il valore double")
            self.value = struct.unpack('>d', double_bytes)[0]
            
        elif self.type == Settings.E_TYPE:
            double_bytes = data_input.read(8)
            if len(double_bytes) < 8:
                raise IOError("Impossibile leggere il valore double")
            self.value = struct.unpack('>d', double_bytes)[0]
            
        elif self.type == Settings.INTERACTION_TYPE:
            double_bytes = data_input.read(8)
            if len(double_bytes) < 8:
                raise IOError("Impossibile leggere il valore double")
            self.value = struct.unpack('>d', double_bytes)[0]
            
        elif self.type == Settings.SLIDING:
            # Legge int (4 bytes)
            int_bytes = data_input.read(4)
            if len(int_bytes) < 4:
                raise IOError("Impossibile leggere no_bits")
            self.no_bits = struct.unpack('>i', int_bytes)[0]
            
            # Crea nuovo bitarray
            self.sliding = bitarray(self.no_bits)
            self.sliding.setall(0)  # inizializza tutti i bit a 0
            
            # Legge i boolean (1 byte ciascuno)
            for i in range(self.no_bits):
                bool_byte = data_input.read(1)
                if len(bool_byte) < 1:
                    raise IOError(f"Impossibile leggere il boolean {i}")
                vl = struct.unpack('?', bool_byte)[0]
                if vl:
                    self.sliding[i] = 1
    
    def write(self, data_output):
        """
        Scrive i campi su un DataOutput (equivalente al metodo write di Java)
        
        Args:
            data_output: oggetto simile a DataOutput (es. BytesIO)
        """
        # Scrive il carattere (2 bytes)
        char_value = ord(self.type) if isinstance(self.type, str) else self.type
        data_output.write(struct.pack('>H', char_value))
        
        if self.type == Settings.EDGE_TYPE:
            data_output.write(struct.pack('>d', self.value))
            
        elif self.type == Settings.STAR_GRAPH:
            raise RuntimeError("There will be no value like this!!!")
            
        elif self.type == Settings.INTERACTION_TYPE:
            data_output.write(struct.pack('>d', self.value))
            
        elif self.type == Settings.E_TYPE:
            data_output.write(struct.pack('>d', self.value))
            
        elif self.type == Settings.D_TYPE:
            data_output.write(struct.pack('>d', self.value))
            
        elif self.type == Settings.C_TYPE:
            data_output.write(struct.pack('>d', self.value))
            
        elif self.type == Settings.SLIDING:
            # Scrive int
            data_output.write(struct.pack('>i', self.no_bits))
            
            # Scrive i boolean
            for i in range(self.no_bits):
                vl = bool(self.sliding[i]) if self.sliding else False
                data_output.write(struct.pack('?', vl))
    
    def __str__(self):
        """
        Equivalente del metodo toString() di Java
        """
        type_char = self.type
        
        if type_char == Settings.EDGE_TYPE:
            return f"{type_char} {self.value}"
            
        elif type_char == Settings.STAR_GRAPH:
            pass  # equivalente del break nel case STAR_GRAPH
            
        elif type_char == Settings.D_TYPE:
            return f"{type_char} {self.value}"
            
        elif type_char == Settings.C_TYPE:
            return f"{type_char} {self.value}"
            
        elif type_char == Settings.E_TYPE:
            return f"{type_char} {self.value}"
            
        elif type_char == Settings.INTERACTION_TYPE:
            return f"{type_char} {self.value}"
            
        elif type_char == Settings.SLIDING:
            s = ""
            for i in range(self.no_bits):
                if self.sliding and self.sliding[i]:
                    s += "1 "
                else:
                    s += "0 "
            return f"{type_char} {s}"
        
        # Equivalente di super.toString() in Java
        return f"<{self.__class__.__name__} object at {hex(id(self))}>"


# # Esempio di utilizzo (assumendo che Settings sia importato)
# if __name__ == "__main__":
#     # Esempio di creazione e utilizzo
#     writable = SpecialEdgeValueWritable()
    
#     # Per testare la serializzazione/deserializzazione:
#     # buffer = BytesIO()
#     # writable.write(buffer)
#     # buffer.seek(0)
#     # new_writable = SpecialEdgeValueWritable()
#     # new_writable.read_fields(buffer)