def join(infile1: csv, infile2: csv) -> csv:
    __kiseru_assigns = {}
    __kiseru_output = run_script("""cat %{infile1} %{infile2} > combined.csv
%{combined} = combined.csv

""", locals(), globals(), __kiseru_assigns)
    set_assignments(__kiseru_output.stdout, __kiseru_assigns)
    return __kiseru_assigns['combined']
