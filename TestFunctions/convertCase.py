import os

filePath = "/validate_columns_test.py"
file = os.getcwd() + filePath
with open(file) as f:
    content = f.readlines()
    for line in content[20:]:
        words = line.split(" ")
        for word in words:
            newWords = word.split("_")
            for i, w in enumerate(newWords):
                if i ==0 :
                    nW = newWords[i]
                else:
                    n = newWords[i][0]
                    r = newWords[i][0].replace(n,n.upper())
                    r + newWords[i][1:]
                    nW = nW+r
            print(nW)

