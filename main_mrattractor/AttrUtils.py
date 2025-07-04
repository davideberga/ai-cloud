class AttrUtils:
    
    @staticmethod
    def read_deg_map_with_key(conf, key):
        """
        Legge una mappa di gradi da un file specificato dalla chiave di configurazione.
        
        Args:
            conf: Oggetto di configurazione (assumiamo abbia un metodo get())
            key: Chiave per ottenere il nome del file dalla configurazione
            
        Returns:
            dict: Mappa {nodo_id: grado} o None in caso di errore
        """
        try:
            name = conf.get(key)
            # Assumiamo che esista una funzione per aprire file dal filesystem
            # (implementata in altri moduli)
            with open_hdfs_file(conf, name) as file:
                deg_map = {}
                for line in file:
                    line = line.strip()
                    if line:
                        args = line.split()
                        deg = int(args[1])
                        u = int(args[0])
                        deg_map[u] = deg
                return deg_map
                
        except Exception as e:
            print(f"Errore: {e}")
            return None
    
    @staticmethod
    def read_deg_map(conf):
        """
        Legge una mappa di gradi dal file specificato da 'deg_file' nella configurazione.
        
        Args:
            conf: Oggetto di configurazione
            
        Returns:
            dict: Mappa {nodo_string: grado}
        """
        name = conf.get("deg_file")
        deg_map = {}
        
        try:
            # Assumiamo che esista una funzione per aprire file dal filesystem
            with open_hdfs_file(conf, name) as file:
                for line in file:
                    line = line.strip()
                    if line:
                        args = line.split()
                        deg = int(args[1])
                        deg_map[args[0]] = deg
                        
        except Exception as e:
            print(f"Errore: {e}")
            
        return deg_map
    
    @staticmethod
    def gen_key(u, v):
        """
        Genera una chiave standardizzata per una coppia di nodi.
        
        Args:
            u: Primo nodo (string)
            v: Secondo nodo (string)
            
        Returns:
            str: Chiave standardizzata "nodo_maggiore nodo_minore"
        """
        uu = int(u)
        vv = int(v)
        
        if uu < vv:
            key = f"{vv} {uu}"
        else:
            key = f"{uu} {vv}"
            
        return key
    
    @staticmethod
    def get_map_edge(conf):
        """
        Restituisce la mappa degli archi con i pesi, caricata in memoria.
        
        Args:
            conf: Oggetto di configurazione
            
        Returns:
            dict: Mappa {chiave_arco: peso}
        """
        name = conf.get("edge_file")
        edge_map = {}
        
        try:
            # Assumiamo che esista una funzione per aprire file dal filesystem
            with open_hdfs_file(conf, name) as file:
                for line in file:
                    line = line.strip()
                    if line:
                        args = line.split()
                        key = AttrUtils.gen_key(args[0], args[1])
                        edge_map[key] = float(args[2])
                        
        except Exception as e:
            print(f"Errore: {e}")
            
        return edge_map


# Funzione helper che assumiamo sia implementata in altri moduli
# per gestire l'apertura di file da Hadoop FileSystem
def open_hdfs_file(conf, file_path):
    """
    Funzione helper per aprire file dal filesystem Hadoop.
    Questa funzione dovrebbe essere implementata in base al tuo setup specifico.
    
    Args:
        conf: Configurazione Hadoop
        file_path: Percorso del file
        
    Returns:
        File object o equivalente per la lettura
    """
    # Implementazione specifica per il tuo ambiente
    # Potrebbe usare pydoop, snakebite, hdfs3, o altre librerie
    pass