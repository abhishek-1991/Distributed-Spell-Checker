javac SpellChecker.java -cp $(hadoop classpath)
jar cf SpellChecker.jar SpellChecker*.class
hadoop jar SpellChecker.jar SpellChecker input
