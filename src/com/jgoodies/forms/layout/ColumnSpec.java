package com.jgoodies.forms.layout;

import com.jgoodies.forms.util.FormUtils;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public final class ColumnSpec extends FormSpec
{
  public static final FormSpec.DefaultAlignment LEFT = FormSpec.LEFT_ALIGN;

  public static final FormSpec.DefaultAlignment CENTER = FormSpec.CENTER_ALIGN;

  public static final FormSpec.DefaultAlignment MIDDLE = CENTER;

  public static final FormSpec.DefaultAlignment RIGHT = FormSpec.RIGHT_ALIGN;

  public static final FormSpec.DefaultAlignment FILL = FormSpec.FILL_ALIGN;

  public static final FormSpec.DefaultAlignment DEFAULT = FILL;

  private static final Map CACHE = new HashMap();

  public ColumnSpec(FormSpec.DefaultAlignment defaultAlignment, Size size, double resizeWeight)
  {
    super(defaultAlignment, size, resizeWeight);
  }

  public ColumnSpec(Size size)
  {
    super(DEFAULT, size, 0.0D);
  }

  /** @deprecated */
  public ColumnSpec(String encodedDescription)
  {
    super(DEFAULT, encodedDescription);
  }

  public static ColumnSpec createGap(ConstantSize gapWidth)
  {
    return new ColumnSpec(DEFAULT, gapWidth, 0.0D);
  }

  public static ColumnSpec decode(String encodedColumnSpec)
  {
    return decode(encodedColumnSpec, LayoutMap.getRoot());
  }

  public static ColumnSpec decode(String encodedColumnSpec, LayoutMap layoutMap)
  {
    FormUtils.assertNotBlank(encodedColumnSpec, "encoded column specification");
    FormUtils.assertNotNull(layoutMap, "LayoutMap");
    String trimmed = encodedColumnSpec.trim();
    String lower = trimmed.toLowerCase(Locale.ENGLISH);
    return decodeExpanded(layoutMap.expand(lower, true));
  }

  static ColumnSpec decodeExpanded(String expandedTrimmedLowerCaseSpec)
  {
    ColumnSpec spec = (ColumnSpec)CACHE.get(expandedTrimmedLowerCaseSpec);
    if (spec == null) {
      spec = new ColumnSpec(expandedTrimmedLowerCaseSpec);
      CACHE.put(expandedTrimmedLowerCaseSpec, spec);
    }
    return spec;
  }

  public static ColumnSpec[] decodeSpecs(String encodedColumnSpecs)
  {
    return decodeSpecs(encodedColumnSpecs, LayoutMap.getRoot());
  }

  public static ColumnSpec[] decodeSpecs(String encodedColumnSpecs, LayoutMap layoutMap)
  {
    return FormSpecParser.parseColumnSpecs(encodedColumnSpecs, layoutMap);
  }

  protected boolean isHorizontal()
  {
    return true;
  }
}