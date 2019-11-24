stream = modeler.script.stream()

log_data = []

def write_log_to_file(file_name, data, mode = "a+"):
    file = open(file_name, mode)
    try:        
        if isinstance(data, list):
            file.write("\n".join(data))
        else:
            file.write(data)
        file.write("\n")
    except:
        print("ERRO gravando log")
    finally:
        file.close()


# TODO
# AJUSTAR ORDEM DOS CAMPOS DE VALIDACAO
# gravar diferenca total por prefixo / rubrica (CD_PRF_DEPE, CD_RBC, COSIF, VL_EVT_REAL)
# gravar diferenca total por prefixo / data    (CD_PRF_DEPE, DT_BAL, VL_DIFF)
# gravar registros incluidos                   (CD_PRF_DEPE, DT_BAL, CD_RBC, COSIF,  VL_EVT_REAL)

def diferencas_por_prefixo_rubrica(table):
    msg = "DIFERENCA POR PREFIXO / RUBRICA"
    size = table.getRowCount()
    rows = []
    rows.append(header(msg))
    for i in range(size):
        row = "%s;%s;%s;%s" % (
            table.getValueAt(i, 0),
            table.getValueAt(i, 1),
            table.getValueAt(i, 2),
            table.getValueAt(i, 3)
        )
        rows.append(row)
    return rows


def header(msg = "", nivel = "-")
    rows = []
    # caso não seja o nível principal, add linha em branco
    if nivel != "=": 
        rows.append("")
    rows.append(msg)
    rows.append(nivel  * len(msg))
    return "\n".join(rows)

def titulo(filename, comp):
    template = """GERAÇÃO DO ARQUIVO DESIF
    ========================
    Competencia:\t%s
    Arquivo:\t`%s`
    Iniciando processamento em %s
    """

    from datetime import datetime
    return template % (comp, filename, datetime.now().strftime("%Y-%m%d %H:%M"))


def data_table(rows, format="tsv"):
    return """[format="%s", options="header"]
|===
%s
|===""" % (format, "\n".join(rows))

def table_to_list(table, delimiter="|"):
    rowCount = table.getRowCount()
    cols = table.getColumnCount()
    rows = []
    
    # nomes das colunas
    names = []
    for n in range(cols):
        names.append(table.getColumnName(n))    
    rows.append(delimiter.join(names))

    for row in range(rowCount):
        r = []
        for column in range(cols):
            value = table.getValueAt(row, column)
            if value and isinstance(value, float):
                r.append(str(table.getValueAt(row, column)).replace(".", ","))
            elif value:
                r.append(str(table.getValueAt(row, column)))
            else:
                r.append("")
        
        rows.append(delimiter.join(r))
        r[:] = []
    return rows


def message(id, msg):
    import urllib
    params = urllib.urlencode({'id': id, 'msg': msg })
    req = urllib.open("http://localhost:8080?%s" % params)
    print(req.read())
    log_data.append(msg)

# Nodes
query           = stream.findByType(None, 'query_cosif7')
table_config    = stream.findByType(None, 'config')
table_job       = stream.findByType(None, 'job')
table_valida    = stream.findByType(None, 'validacao')
table_mov       = stream.findByType(None, 'movimento')
file_output     = stream.findByType(None, 'arquivo_final')
table_valida_debitos = stream.findByType(None, 'valida_debitos')

# Meses executados
jobs = []
table_job.run(jobs)             # buscando Job=1, pode ser um parametro
jobs = jobs[0].getRowSet()

"""
CREATE TABLE DESIF_JOB_CONFIG (
    CD_JOB INT,
    CD_MM_CNT INT,
    NM_JOB VARCHAR(32),
    IN_JOB_ATIVO SMALLINT DEFAULT 0,
    PRIMARY KEY (CD_JOB, CD_MM_CNT)
);
INSERT INTO DESIF_JOB_CONFIG VALUES (1, 12019, 'rio_rj');
INSERT INTO DESIF_JOB_CONFIG VALUES (1, 22019, 'rio_rj');
INSERT INTO DESIF_JOB_CONFIG VALUES (1, 32019, 'rio_rj');
INSERT INTO DESIF_JOB_CONFIG VALUES (1, 42019, 'rio_rj');
INSERT INTO DESIF_JOB_CONFIG VALUES (1, 52019, 'rio_rj');
INSERT INTO DESIF_JOB_CONFIG VALUES (1, 62019, 'rio_rj');
"""

for j in range(jobs.getRowCount()):
    from datetime import datetime
    file_name = "desif_{0}_{1:06d}_{2}.csv".format(
        jobs.getValueAt(0, 2), 
        jobs.getValueAt(0, 1),
        datetime.now().strftime("%Y%m%d-%H%M")
    )

    stream.setParameterValue('CD_MM_CNT', jobs.getValueAt(j, 0))
    
    config = []
    table_config.run(config)
    config = config[0].getRowSet()
    stream.setParameterValue('TAB_MOV_ANTERIOR', config.getValueAt(0, 1))
    stream.setParameterValue('TAB_MOV_ATUAL', config.getValueAt(0, 2))
    
    # Validação
    # CADS tem CACHE????
    validacao = []
    stream.runSelected([table_valida, table_valida_debitos], validacao)

    # totais por prefixo
    #table_valida.run(validacao)
    validacao = validacao[0].getRowSet()
    # verifica se todos os registros possuem VL_EVT = 0
    for i in range(validacao.getRowCount()):
        value = int(validacao.getValueAt(i, 1))
        if value != 0:
            message(-1, "erro na validacao")
            exit 1

    # Soma dos débitos
    #validacao = []
    #table_valida_debitos.run(validacao)
    debitos_invalidos = validacao[1].getRowSet().getRowCount()
    if debitos_invalidos:
        message(debitos_invalidos, "erro na validacao da soma dos débitos")
        exit 1

    # Roda fluxo com saida em arquivo
    
    file_output.setPropertyValue('full_filename', file_name)
    file_output.run()

    query.flushCache()

    message(0, "geração do arquivo [ %s ] completa!" % file_name)
    write_log_to_file("c:\Temp\%s.log" % file_name, log_data)
    log_data.clear()
