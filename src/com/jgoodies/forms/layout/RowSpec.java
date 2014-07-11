package com.jgoodies.forms.layout;

import com.jgoodies.forms.util.FormUtils;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public final class RowSpec extends FormSpec
{
  public static final FormSpec.DefaultAlignment TOP = FormSpec.TOP_ALIGN;

  public static final FormSpec.DefaultAlignment CENTER = FormSpec.CENTER_ALIGN;

  public static final FormSpec.DefaultAlignment BOTTOM = FormSpec.BOTTOM_ALIGN;

  public static final FormSpec.DefaultAlignment FILL = FormSpec.FILL_ALIGN;

  public static final FormSpec.DefaultAlignment DEFAULT = CENTER;

  private static final Map CACHE = new HashMap();

  public RowSpec(FormSpec.DefaultAlignment defaultAlignment, Size size, double resizeWeight)
  {
    super(defaultAlignment, size, resizeWeight);
  }

  public RowSpec(Size size)
  {
    super(DEFAULT, size, 0.0D);
  }

  /** @deprecated */
  public RowSpec(String encodedDescription)
  {
    super(DEFAULT, encodedDescription);
  }

  public static RowSpec createGap(ConstantSize gapHeight)
  {
    return new RowSpec(DEFAULT, gapHeight, 0.0D);
  }

  public static RowSpec decode(String encodedRowSpec)
  {
    return decode(encodedRowSpec, LayoutMap.getRoot());
  }

  public static RowSpec decode(String encodedRowSpec, LayoutMap layoutMap)
  {
    FormUtils.assertNotBlank(encodedRowSpec, "encoded row specification");
    FormUtils.assertNotNull(layoutMap, "LayoutMap");
    String trimmed = encodedRowSpec.trim();
    String lower = trimmed.toLowerCase(Locale.ENGLISH);
    return decodeExpanded(layoutMap.expand(lower, false));
  }

  static RowSpec decodeExpanded(String expandedTrimmedLowerCaseSpec)
  {
    RowSpec spec = (RowSpec)CACHE.get(expandedTrimmedLowerCaseSpec);
    if (spec == null) {
      spec = new RowSpec(expandedTrimmedLowerCaseSpec);
      CACHE.put(expandedTrimmedLowerCaseSpec, spec);
    }
    return spec;
  }

  public static RowSpec[] decodeSpecs(String encodedRowSpecs)
  {
    return decodeSpecs(encodedRowSpecs, LayoutMap.getRoot());
  }

  public static RowSpec[] decodeSpecs(String encodedRowSpecs, LayoutMap layoutMap)
  {
    return FormSpecParser.parseRowSpecs(encodedRowSpecs, layoutMap);
  }

  protected boolean isHorizontal()
  {
    return false;
  }
}