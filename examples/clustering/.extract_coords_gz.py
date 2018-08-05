def extract_coords_gz(infile) -> csv:
    __kiseru_assigns = {}
    __kiseru_output = run_script("""gunzip -c %{infile} > tmp.csv
sed -i -e '1,3d' tmp.csv
cat tmp.csv | cut -d, -f2,3 > 2017.csv
%{outfile}=2017.csv

""", locals(), globals(), __kiseru_assigns)
    set_assignments(__kiseru_output.stdout, __kiseru_assigns)
    return __kiseru_assigns['outfile']
