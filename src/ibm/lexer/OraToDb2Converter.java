package ibm.lexer;

import ibm.IBMExtractUtilities;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OraToDb2Converter
{
  private static Parser pt;

  private static void initializePlSqlParsedTokens(String oraSql)
  {
    pt = new Parser(new Db2PlSqlLexer());
    pt.parse(pt.getBufferContents(oraSql));
  }

  private static String buildTrigger(String triggerName, boolean inTrigger, Token start, Token end, String type, Token[] pos)
  {
    Parser.TokenIterator iter = pt.getTokens(start.start + 1, end.start + 1);
    StringBuffer sb = new StringBuffer();
    boolean once = true;
    String newTriggerName = IBMExtractUtilities.getStringName(triggerName, "_" + type);

    while (iter.hasNext())
    {
      Token t = iter.next();

      if (t.start == pos[0].start)
      {
        sb.append(newTriggerName + " ");
        continue; } if ((t.start > pos[1].start) && (t.start < pos[2].start))
      {
        if ((!once) || 
          (!t.text.equalsIgnoreCase(type))) {
          continue;
        }
        sb.append(" ");
        sb.append(type);
        sb.append(" ");

        boolean skip = false;
        if (t.text.equalsIgnoreCase("UPDATE"))
        {
          t = iter.next();
          while (!t.text.equalsIgnoreCase("ON"))
          {
            if (t.text.equalsIgnoreCase("OR"))
            {
              skip = true;
            }
            else if (t.text.equalsIgnoreCase("OF"))
            {
              skip = false;
            }
            if ((!skip) && (t.type != TokenType.LINE) && (t.type != TokenType.WHITESPACE))
            {
              sb.append(t.text + " ");
            }
            t = iter.next();
          }

        }

        while (!t.text.equalsIgnoreCase("ON"))
        {
          t = iter.next();
        }

        sb.append(t.text);
        once = false;
        continue;
      }

      if (t.type == TokenType.DELETEDTEXT)
      {
        continue;
      }
      if (t.type == TokenType.COMMENT)
      {
        if (t.text.startsWith("--#SET :"))
        {
          if (!inTrigger)
          {
            String comment = t.text;
            String trigName = t.text.substring(t.text.lastIndexOf(":") + 1);
            comment = comment.replaceFirst(trigName, newTriggerName.replaceAll("\"", ""));
            sb.append(comment + IBMExtractUtilities.linesep);
            continue;
          }

          sb.append(t.text);

          continue;
        }

        sb.append(t.text);

        continue;
      }if (t.type == TokenType.LINE)
      {
        sb.append(IBMExtractUtilities.linesep);
        continue;
      }

      sb.append(t.text);
    }

    sb.append(IBMExtractUtilities.linesep);
    return sb.toString();
  }

  public static String getSplitTriggers(String oraSql)
  {
    boolean beginTrigger = true; boolean insertTrigger = false; boolean updateTrigger = false; boolean deleteTrigger = false;
    Token beginToken = null; Token endToken = null;

    String triggerName = "";

    Token[] pos = new Token[3];
    StringBuffer sb = new StringBuffer();

    if ((oraSql == null) || (oraSql.length() == 0))
    {
      return oraSql;
    }
    try
    {
      initializePlSqlParsedTokens(oraSql);
      Parser.TokenIterator iter = pt.getTokens();

      boolean inTrigger = false;
      while (iter.hasNext())
      {
        if (beginTrigger)
        {
          beginToken = iter.next();
          beginTrigger = false;
          insertTrigger = false;
          updateTrigger = false;
          deleteTrigger = false;
          triggerName = "";
        }
        Token t = iter.next();
        if ((t.type == TokenType.KEYWORD) && (t.getText().matches("(?i).*TRIGGER")))
        {
          inTrigger = true;
          t = iter.nextToken();
          if ((t.type == TokenType.STRING) || (t.type == TokenType.IDENTIFIER))
          {
            triggerName = t.text;
            pos[0] = t;
            t = iter.nextToken();
            if ((t.type == TokenType.OPERATOR) && (t.getText().equals(".")))
            {
              t = iter.nextToken();
              if ((t.type == TokenType.STRING) || (t.type == TokenType.IDENTIFIER))
              {
                pos[0] = t;
                triggerName = t.getText();
              }
            }
          }
        }

        if ((!inTrigger) || (t.type != TokenType.KEYWORD) || ((!t.getText().equalsIgnoreCase("BEFORE")) && (!t.getText().equalsIgnoreCase("AFTER")) && (!t.getText().equalsIgnoreCase("INSTEAD OF"))))
        {
          continue;
        }

        pos[1] = t;
        do
        {
          t = iter.next(TokenType.KEYWORD);
          if (t.type != TokenType.KEYWORD)
            continue;
          if (t.getText().equalsIgnoreCase("INSERT"))
          {
            insertTrigger = true;
          }
          if (t.getText().equalsIgnoreCase("UPDATE"))
          {
            updateTrigger = true;
          }
          if (!t.getText().equalsIgnoreCase("DELETE"))
            continue;
          deleteTrigger = true;
        }

        while ((t.type == TokenType.KEYWORD) && (!t.getText().equalsIgnoreCase("ON")));

        pos[2] = t;
        endToken = iter.next(TokenType.SQLTERMINATOR);
        inTrigger = false;
        beginTrigger = true;
        if (insertTrigger)
        {
          sb.append(buildTrigger(triggerName, inTrigger, beginToken, endToken, "INSERT", pos));
        }
        if (updateTrigger)
        {
          sb.append(buildTrigger(triggerName, inTrigger, beginToken, endToken, "UPDATE", pos));
        }
        if (!deleteTrigger)
          continue;
        sb.append(buildTrigger(triggerName, inTrigger, beginToken, endToken, "DELETE", pos));
      }

    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
    return sb.toString();
  }

  public static String getDb2PlSql(String oraSql)
  {
    StringBuffer sb = new StringBuffer();

    if ((oraSql == null) || (oraSql.length() == 0))
    {
      return oraSql;
    }

    initializePlSqlParsedTokens(oraSql);
    Parser.TokenIterator iter = pt.getTokens();

    while (iter.hasNext())
    {
      Token t = iter.next();
      sb.append(t.getText());
    }
    return sb.toString();
  }

  private static boolean checkOpenCursor(Parser p, String cursorName)
  {
    boolean found = false;

    Parser.TokenIterator iter = p.getTokens();

    while (iter.hasNext())
    {
      Token t = iter.next();
      if ((t.type != TokenType.WHITESPACE) && (t.text.equalsIgnoreCase("OPEN")))
      {
        t = iter.next();
        if (t.type == TokenType.WHITESPACE)
          t = iter.next();
        if (t.text.equalsIgnoreCase(cursorName))
        {
          found = true;
        }
      }
      if ((t.type == TokenType.WHITESPACE) || ((!t.text.equalsIgnoreCase("CLOSE")) && (!t.text.equalsIgnoreCase("FETCH")))) {
        continue;
      }
      t = iter.next();
      if (t.type == TokenType.WHITESPACE)
        t = iter.next();
      if (!t.text.equalsIgnoreCase(cursorName))
        continue;
      found = false;
    }

    return found;
  }

  public static String fixiDB2CursorForReturn(String iDB2SQL)
  {
    String word = ""; String uword = "";

    StringBuffer sb = new StringBuffer();

    if ((iDB2SQL == null) || (iDB2SQL.length() == 0))
    {
      return iDB2SQL;
    }
    try
    {
      pt = new Parser(new IDb2PlSqlLexer());
      pt.parse(pt.getBufferContents(iDB2SQL));
      Parser.TokenIterator iter = pt.getTokens();
      while (iter.hasNext())
      {
        String tok1 = "";
        Token t = iter.next();
        word = t.text.toUpperCase();
        if (t.type == TokenType.LINE)
        {
          sb.append(IBMExtractUtilities.linesep);
          continue;
        }if (t.type == TokenType.IDB2WORDS)
        {
          word = IBMExtractUtilities.removeLineChar(word);
          if (word.matches("DECLARE\\s+\\w+\\s+CURSOR\\s+WITH\\s+RETURN\\s+.*"))
          {
            sb.append(t.text);
            continue; } if (word.matches("DECLARE\\s+\\w+\\s+CURSOR\\s+.*\\;\\s*"))
          {
            String regex = "DECLARE\\s+(\\w+)\\s+CURSOR\\s+.*";

            Matcher matcher = null;
            Pattern pattern = Pattern.compile(regex, 10);
            matcher = pattern.matcher(word);
            if (matcher.matches())
            {
              tok1 = matcher.group(1);
              if (checkOpenCursor(pt, tok1))
              {
                uword = t.text.toUpperCase();
                int pos2 = word.indexOf(tok1);
                int pos = uword.indexOf("CURSOR", pos2 + tok1.length());
                word = t.text.substring(0, pos + 6) + " WITH RETURN " + t.text.substring(pos + 7);
                sb.append(word);
              } else {
                sb.append(t.text);
              }
            }
            continue;
          }sb.append(t.text);
          continue;
        }
        sb.append(t.text);
      }
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
    return sb.toString();
  }

  public static String fixiDB2Procedures(String iDB2SQL)
  {
    String word = "";

    StringBuffer sb = new StringBuffer();

    if ((iDB2SQL == null) || (iDB2SQL.length() == 0))
    {
      return iDB2SQL;
    }
    try
    {
      pt = new Parser(new IDb2PlSqlLexer());
      pt.parse(pt.getBufferContents(iDB2SQL));
      Parser.TokenIterator iter = pt.getTokens();

      while (iter.hasNext())
      {
        String tok3;
        String tok2;
        String tok1 = tok2 = tok3 = "";
        Token t = iter.next();
        word = t.text.toUpperCase();
        if (t.type == TokenType.COMMENT)
        {
          sb.append(t.text);

          continue; } if (t.type == TokenType.LINE)
        {
          sb.append(IBMExtractUtilities.linesep);
          continue;
        }if (t.type == TokenType.IDB2WORDS)
        {
          word = IBMExtractUtilities.removeLineChar(word);
          if ((word.equals("CHARACTER_LENGTH")) || (word.equals("CHAR_LENGTH"))) {
            word = "LENGTH";
          } else if (word.startsWith("NOW"))
          {
            word = "CURRENT_TIMESTAMP";
          }
          else if (word.matches(".*(NOW\\s+\\(\\s+\\)).*"))
          {
            String regex = "(.*)(NOW\\s+\\(\\s+\\))(.*)";

            Matcher matcher = null;
            Pattern pattern = Pattern.compile(regex, 10);
            matcher = pattern.matcher(word);
            if (matcher.matches())
            {
              tok1 = matcher.group(1);
              tok2 = matcher.group(2);
              tok3 = matcher.group(3);
              word = tok1 + "CURRENT_TIMESTAMP" + tok3;
            }
          }
          else if (word.equals("SUBSTRING")) {
            word = "SUBSTR";
          } else if (word.equals("IFNULL")) {
            word = "COALESCE";
          } else if (word.startsWith("POSITION"))
          {
            String regex = "POSITION\\s*\\(\\s*(\\'.*\\'|\\w*)\\s+(IN)\\s+(\\w+)\\s*\\)\\s*";

            Matcher matcher = null;
            Pattern pattern = Pattern.compile(regex, 10);
            matcher = pattern.matcher(word);
            if (matcher.matches())
            {
              tok1 = matcher.group(1);
              tok2 = matcher.group(3);
              word = "INSTR(" + tok2 + "," + tok1 + ")";
            }
          }
          else if (word.toUpperCase().startsWith("CHAR"))
          {
            String regex = "CHAR\\s*\\(\\s*(\\d+)\\s*\\)\\s*";

            Matcher matcher = null;
            Pattern pattern = Pattern.compile(regex, 10);
            matcher = pattern.matcher(word);
            if (matcher.matches())
            {
              tok1 = matcher.group(1);
              try
              {
                int val = Integer.valueOf(tok1).intValue();
                if (val > 254)
                  word = "VARCHAR (" + val + ")";
              } catch (Exception e) {
              }
            }
          } else if (word.matches("GET\\s+\\DIAGNOSTICS.*\\;"))
          {
            String regex = "GET\\s+DIAGNOSTICS\\s+EXCEPTION\\s+1\\s+(\\w+)\\s*=\\s*MESSAGE_TEXT\\s*\\,\\s*(\\w+)\\s*=\\s*MESSAGE_LENGTH\\s*\\;";

            Matcher matcher = null;
            Pattern pattern = Pattern.compile(regex, 10);
            matcher = pattern.matcher(word);
            if (matcher.matches())
            {
              tok1 = matcher.group(1);
              tok2 = matcher.group(2);
              word = "GET DIAGNOSTICS EXCEPTION 1 " + tok1 + " = MESSAGE_TEXT; SET " + tok2 + " = LENGTH(" + tok1 + ");";
            }
          }
          sb.append(word);
          continue;
        }
        sb.append(t.text);
      }
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
    return sb.toString();
  }

  private static void processiDB2ProceduresForCursors(String outputDirectory, String fileName)
  {
    String linesep = IBMExtractUtilities.linesep;
    String inFileName = outputDirectory + fileName;

    String origFileName = outputDirectory + fileName.substring(0, fileName.lastIndexOf('.')) + "_Original" + fileName.substring(fileName.lastIndexOf('.'));

    StringBuffer buffer = new StringBuffer();
    try
    {
      IBMExtractUtilities.copyFile(new File(inFileName), new File(origFileName));
      BufferedWriter out = new BufferedWriter(new FileWriter(inFileName, false));
      BufferedReader in = new BufferedReader(new FileReader(origFileName));

      boolean collectCode = false;
      String line;
      while ((line = IBMExtractUtilities.readLine(in)) != null)
      {
        if (line.startsWith("--#SET :"))
        {
          collectCode = true;
        }
        if (collectCode)
        {
          if (line.startsWith("--#SET :"))
          {
            collectCode = true;
            out.write(line + linesep);
            continue;
          }if (line.equals("@"))
          {
            String text = fixiDB2CursorForReturn(buffer.toString());
            out.write(text);
            out.write(line + linesep);
            collectCode = false;
            buffer.setLength(0);
            continue;
          }
          buffer.append(line + linesep);
          continue;
        }out.write(line + linesep);
      }

      in.close();
      out.close();
      IBMExtractUtilities.log("File " + inFileName + " saved as " + origFileName);
    }
    catch (IOException ex)
    {
      IBMExtractUtilities.log("File " + inFileName + " was not found. Skipping it");
      ex.printStackTrace();
    }
  }

  public static void main(String[] args)
  {
    String fileName = "db2procedure2.db2";
    String OUTPUT_DIR = "C:\\Vikram\\Prospects\\FirstAmerican\\";

    processiDB2ProceduresForCursors(OUTPUT_DIR, fileName);
  }
}