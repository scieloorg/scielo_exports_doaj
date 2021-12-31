# scielo_exports_doaj

__scielo-export__
```bash
usage: scielo-export [-h] [--loglevel LOGLEVEL] [--issns ISSNS] --output OUTPUT {doaj} ...

Exportador de documentos

optional arguments:
  -h, --help           show this help message and exit
  --loglevel LOGLEVEL
  --issns ISSNS        Caminho para arquivo de ISSNs gerenciados
  --output OUTPUT      Caminho para arquivo de resultado da exportação

Index:
  {doaj}
    doaj               Base de indexação DOAJ
```

__scielo-export doaj__
```bash
usage: scielo-export doaj [-h] {export,update,get,delete} ...

optional arguments:
  -h, --help            show this help message and exit

DOAJ Command:
  {export,update,get,delete}
    export              Exporta documentos
    update              Atualiza documentos
    get                 Obtém documentos
    delete              Deleta documentos
```

__scielo-export doaj export__
```bash
usage: scielo-export doaj export [-h] [--from-date FROM_DATE] [--until-date UNTIL_DATE] [--collection COLLECTION] [--pid PID] [--pids PIDS] [--connection CONNECTION] [--domain DOMAIN] [--bulk]

optional arguments:
  -h, --help            show this help message and exit
  --from-date FROM_DATE
                        Data inicial de processamento
  --until-date UNTIL_DATE
                        Data final de processamento
  --collection COLLECTION
                        Coleção do(s) documento(s) publicados
  --pid PID             PID do documento
  --pids PIDS           Caminho para arquivo com lista de PIDs de documentos a exportar
  --connection CONNECTION
                        Tipo de conexão com Article Meta: Restful ou Thrift
  --domain DOMAIN       Endereço de conexão com Article Meta
  --bulk                Exporta documentos em lote
```
