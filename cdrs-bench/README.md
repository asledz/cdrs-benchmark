# cdrs-benchmark


# Opis benchmarku

Benchmark ma za zadanie wykonać określoną ilość komend CQL-owych (CQL statement) w jak najkrótszym czasie.

Będziemy mierzyć czas użyty przez benchmark poprzez komendę `time` z Linuxa.

## Parametry

Zdefiniowane są następujące parametry, które można określić przed uruchomieniem. Oto najważniejsze z nich:

- ilość operacji do wykonania,
- współbieżność - czyli ile operacji może się wykonywać naraz,
- tryb (write, read, albo mixed - opisane niżej)

Pozostałe parametry tyczą się konfiguracji bazy danych i tego, jak są wykonywane zapytania:

- replication factor - parametr keyspace'a, opisany niżej,
- consistency level - consistency level użyty do zapisów i odczytów.
- compression - jakiego algorytmu kompresji użyć do komunikacji z bazą danych.

### Schema

Benchmark powinien operować na keyspace o następującej schemie:

    CREATE KEYSPACE IF NOT EXISTS ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': (replication factor)}

gdzie (replication factor) to parametr podany do benchmarka.

Wszystkie operacje wykonywane są na tej samej tabelce, o konkretnej schemie:

    CREATE TABLE ks.t (pk bigint PRIMARY KEY, v1 bigint, v2 bigint)

Nazwy keyspace'a i tabelki nie mają większego znaczenia.

#### Przygotowanie schemy

Benchmark powinien udostępniać komendę która pozwala na postawienie keyspace'a i tabelki. W pomiarach nie chcemy uwzględniać czasu poświęconego na tworzenie ich.

### Tryby

Niech `T` = ilość operacji do wykonania.

Benchmark można uruchomić w jednym z trzech trybów:

- write - każda komenda to zapis do tabelki:

      INSERT INTO ks_rust_scylla_bench.t (pk, v1, v2) VALUES (?, ?, ?)

  Przy zapisywaniu do klucza `pk`, `v1` i `v2` powinny wynosić odpowiednio `2 * pk` i `3 * pk`.

  W tym trybie, benchmark powinien wykonać jeden zapis dla każdego `pk ∈ [0, T)`. Kolejność zapisów nie ma znaczenia.

- read - każda komenda to odczyt z tabelki:

      SELECT v1, v2 FROM ks_rust_scylla_bench.t WHERE pk = ?

  Po odczytaniu wiersza, benchmark powinien zweryfikować że `v1 == 2 * pk` i `v2 == 3 * pk`.

  W tym trybie, benchmark powinien wykonać jeden odczyt dla każdego `pk ∈ [0, T)`. Kolejność odczytów nie ma znaczenia.

- mixed - każda operacja to albo zapis, albo odczyt, zdefiniowane jak w powyższych trybach.

  Dla dla każdego `pk ∈ [0, T/2)`, wiersz powinien być zapisany, a potem od razu odczytany. Kolejność dla różnych `pk` nie ma znaczenia, ale dla konkretnego `pk` zapis powinien się odbyć przed odczytem.

### Istniejące benchmarki

Można się wzorować na następujących benchmarkach:

- scylla-rust-driver: [link do brancha](https://github.com/psarna/scylla-rust-driver/tree/piodul/benchmark)
- gocql: [link do repo](https://github.com/piodul/golang-bench)
