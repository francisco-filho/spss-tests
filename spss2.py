stream = modeler.script.stream()

task = modeler.script.session().getTaskRunner()

def header(msg = "", nivel = "-"):
    rows = []
    if nivel != "=": 
        rows.append("")
    rows.append(msg)
    rows.append(nivel  * len(msg))
    return "\n".join(rows)

def titulo(filename, comp):
    template = """GERAÇÃO DO ARQUIVO DESIF\n========================
Competencia:\t%s\nArquivo:\t`%s`\nIniciando processamento em %s
========================\n"""
    from datetime import datetime
    return template % (comp, filename, datetime.now().strftime("%Y-%m%d %H:%M"))

def write_log_to_file(file_name, data, mode = "a+"):
    from datetime import datetime
    
    file = open("%s%s.txt" % (file_name, datetime.now().strftime("%Y%m%d%H%M")), mode)
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


invoice_table = stream.findByType(None, 'InvoiceTable')

def table_to_list(table, delimiter = ";"):
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

def data_table(rows, format="tsv"):
    return """[format="%s", options="header"]\n|===\n%s\n|===""" % (format, "\n".join(rows))

results = []
invoice_table.run(results)

lines = []
lines.append(header("hello world", "-"))
lines.append("")
lines.append(data_table(table_to_list(results[0]), "|"))

lines.append(titulo("/modeler/data/teste/file.txt", "012019"))
lines.append("")
lines.append(data_table(table_to_list(results[0])))

write_log_to_file("c:\\temp\\table1", lines)