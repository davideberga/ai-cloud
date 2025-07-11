#!/usr/bin/env python3
"""
Script principale per l'esecuzione dell'algoritmo Jaccard
"""

import sys
import attractor.GraphUtils as GraphUtils  # Assumendo che JaccardInit sia implementata in jaccard_init.py


def main():
    """
    Funzione principale che gestisce gli argomenti da riga di comando
    ed esegue l'algoritmo Jaccard
    
    Args da riga di comando:
        args[0]: Nome del file del grafo
        args[1]: Numero di vertici
        args[2]: Numero di archi
        args[3]: Valore lambda
    """
    try:
        # Controllo del numero di argomenti
        if len(sys.argv) != 5:  # sys.argv[0] è il nome del script
            print("Uso: python jaccard_starting_point.py <graphfile> <no_vertices> <no_edges> <lambda>")
            sys.exit(1)
        
        # Parsing degli argomenti
        graphfile = sys.argv[1]
        no_vertices = int(sys.argv[2])
        no_edges = int(sys.argv[3])
        lambda_ = float(sys.argv[4])
        
        # Creazione ed esecuzione dell'algoritmo
        single_attractor = GraphUtils(graphfile, no_vertices, no_edges, lambda_)
        single_attractor.execute()
        
    except ValueError as e:
        print(f"Errore nel parsing degli argomenti: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Errore durante l'esecuzione: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()