def extract_coords_xls(infile) -> xls:
    __kiseru_assigns = {}
    outfile = '2016.xls'
    df = pd.read_excel(infile, usecols=[1, 2])
    df.to_excel(outfile, index=False, header=False)
    return outfile
