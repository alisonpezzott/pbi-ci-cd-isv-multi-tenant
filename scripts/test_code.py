import chardet

path = r"C:\Users\alison.pezzott\Documents\projects\pbi-ci-cd-isv-multi-tenant\src\AdventureWorks.SemanticModel\definition\expressions.tmdl"

with open(path, 'r', encoding='windows-1252') as f:
    result = f.read()

print(result)