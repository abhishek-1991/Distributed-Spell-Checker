import os, time, re
os.getcwd()

startTime = time.time()

wordFile = open("..\\src\\words.txt","r")
words = wordFile.read()
print("Words in dictionary:",len(words))

inputDoc = open("..\\src\\guten.txt", "r", encoding="utf-8")
doc = inputDoc.read().split()
print("Words in file:",len(doc))


## Processing the input document
def is_number(x):
    #checking if number is int or not
    try:
        int(x)
        return True
    except (TypeError, ValueError):
        pass
    return False

processedInput = list()
for word in doc:
    if not is_number(word):
        if not "@" in word:
            if not "www." in word:
                if len(re.sub('[^A-Za-z0-9]+', '', word)) > 1:
                    processedInput.append(re.sub('[^A-Za-z0-9]+', '', word))


misspelledWords = list()
i = 0
for word in processedInput:
#     i += 1
#     print(i, end=", ")
    if word.lower() not in words:
        misspelledWords.append(word)
print("Total misspelled words =",len(misspelledWords))
print("Total execution time = %s sec"%(time.time() - startTime))


with open("..//results//outputPython.txt", "w") as outFile:
    for word in misspelledWords:
        outFile.write(word)
        outFile.write("\n")
print ("Incorrect words written to outputPython.txt")

