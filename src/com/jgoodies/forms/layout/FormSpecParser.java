package com.jgoodies.forms.layout;

import com.jgoodies.forms.util.FormUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class FormSpecParser
{
  private static final Pattern MULTIPLIER_PREFIX_PATTERN = Pattern.compile("\\d+\\s*\\*\\s*\\(");

  private static final Pattern DIGIT_PATTERN = Pattern.compile("\\d+");
  private final String source;
  private final LayoutMap layoutMap;

  private FormSpecParser(String source, String description, LayoutMap layoutMap, boolean horizontal)
  {
    FormUtils.assertNotNull(source, description);
    FormUtils.assertNotNull(layoutMap, "LayoutMap");
    this.layoutMap = layoutMap;
    this.source = this.layoutMap.expand(source, horizontal);
  }

  static ColumnSpec[] parseColumnSpecs(String encodedColumnSpecs, LayoutMap layoutMap)
  {
    FormSpecParser parser = new FormSpecParser(encodedColumnSpecs, "encoded column specifications", layoutMap, true);

    return parser.parseColumnSpecs();
  }

  static RowSpec[] parseRowSpecs(String encodedRowSpecs, LayoutMap layoutMap)
  {
    FormSpecParser parser = new FormSpecParser(encodedRowSpecs, "encoded column specifications", layoutMap, false);

    return parser.parseRowSpecs();
  }

  private ColumnSpec[] parseColumnSpecs()
  {
    List encodedColumnSpecs = split(this.source, 0);
    int columnCount = encodedColumnSpecs.size();
    ColumnSpec[] columnSpecs = new ColumnSpec[columnCount];
    for (int i = 0; i < columnCount; i++) {
      String encodedSpec = (String)encodedColumnSpecs.get(i);
      columnSpecs[i] = ColumnSpec.decodeExpanded(encodedSpec);
    }
    return columnSpecs;
  }

  private RowSpec[] parseRowSpecs()
  {
    List encodedRowSpecs = split(this.source, 0);
    int rowCount = encodedRowSpecs.size();
    RowSpec[] rowSpecs = new RowSpec[rowCount];
    for (int i = 0; i < rowCount; i++) {
      String encodedSpec = (String)encodedRowSpecs.get(i);
      rowSpecs[i] = RowSpec.decodeExpanded(encodedSpec);
    }
    return rowSpecs;
  }

  private List split(String expression, int offset)
  {
    List encodedSpecs = new ArrayList();
    int parenthesisLevel = 0;
    int bracketLevel = 0;
    int length = expression.length();
    int specStart = 0;

    boolean lead = true;
    for (int i = 0; i < length; i++) {
      char c = expression.charAt(i);
      if ((lead) && (Character.isWhitespace(c))) {
        specStart++;
      }
      else {
        lead = false;
        if ((c == ',') && (parenthesisLevel == 0) && (bracketLevel == 0)) {
          String token = expression.substring(specStart, i);
          addSpec(encodedSpecs, token, offset + specStart);
          specStart = i + 1;
          lead = true;
        } else if (c == '(') {
          if (bracketLevel > 0) {
            fail(offset + i, "illegal '(' in [...]");
          }
          parenthesisLevel++;
        } else if (c == ')') {
          if (bracketLevel > 0) {
            fail(offset + i, "illegal ')' in [...]");
          }
          parenthesisLevel--;
          if (parenthesisLevel < 0)
            fail(offset + i, "missing '('");
        }
        else if (c == '[') {
          if (bracketLevel > 0) {
            fail(offset + i, "too many '['");
          }
          bracketLevel++;
        } else if (c == ']') {
          bracketLevel--;
          if (bracketLevel < 0)
            fail(offset + i, "missing '['");
        }
      }
    }
    if (parenthesisLevel > 0) {
      fail(offset + length, "missing ')'");
    }
    if (bracketLevel > 0) {
      fail(offset + length, "missing ']");
    }
    if (specStart < length) {
      String token = expression.substring(specStart);
      addSpec(encodedSpecs, token, offset + specStart);
    }
    return encodedSpecs;
  }

  private void addSpec(List encodedSpecs, String expression, int offset)
  {
    String trimmedExpression = expression.trim();
    Multiplier multiplier = multiplier(trimmedExpression, offset);
    if (multiplier == null) {
      encodedSpecs.add(trimmedExpression);
      return;
    }
    List subTokenList = split(multiplier.expression, offset + multiplier.offset);
    for (int i = 0; i < multiplier.multiplier; i++)
      encodedSpecs.addAll(subTokenList);
  }

  private Multiplier multiplier(String expression, int offset)
  {
    Matcher matcher = MULTIPLIER_PREFIX_PATTERN.matcher(expression);
    if (!matcher.find()) {
      return null;
    }
    if (matcher.start() > 0) {
      fail(offset + matcher.start(), "illegal multiplier position");
    }
    Matcher digitMatcher = DIGIT_PATTERN.matcher(expression);
    if (!digitMatcher.find()) {
      return null;
    }
    String digitStr = expression.substring(0, digitMatcher.end());
    int number = 0;
    try {
      number = Integer.parseInt(digitStr);
    } catch (NumberFormatException e) {
      fail(offset, e);
    }
    if (number <= 0) {
      fail(offset, "illegal 0 multiplier");
    }
    String subexpression = expression.substring(matcher.end(), expression.length() - 1);
    return new Multiplier(number, subexpression, matcher.end());
  }

  public static void fail(String source, int index, String description)
  {
    throw new FormLayoutParseException(message(source, index, description));
  }

  private void fail(int index, String description)
  {
    throw new FormLayoutParseException(message(this.source, index, description));
  }

  private void fail(int index, NumberFormatException cause)
  {
    throw new FormLayoutParseException(message(this.source, index, "Invalid multiplier"), cause);
  }

  private static String message(String source, int index, String description)
  {
    StringBuffer buffer = new StringBuffer(10);
    buffer.append('\n');
    buffer.append(source);
    buffer.append('\n');
    for (int i = 0; i < index; i++) {
      buffer.append(' ');
    }
    buffer.append('^');
    buffer.append(description);
    String message = buffer.toString();
    throw new FormLayoutParseException(message);
  }

  static final class Multiplier
  {
    final int multiplier;
    final String expression;
    final int offset;

    Multiplier(int multiplier, String expression, int offset)
    {
      this.multiplier = multiplier;
      this.expression = expression;
      this.offset = offset;
    }
  }

  public static final class FormLayoutParseException extends RuntimeException
  {
    FormLayoutParseException(String message)
    {
      super();
    }

    FormLayoutParseException(String message, Throwable cause) {
      super(cause);
    }
  }
}