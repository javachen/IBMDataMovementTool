package com.jgoodies.forms.layout;

import java.awt.Component;
import java.awt.Container;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Locale;

import com.jgoodies.forms.util.DefaultUnitConverter;
import com.jgoodies.forms.util.UnitConverter;

public final class Sizes {
	static final class ComponentSize implements Size, Serializable {

		static ComponentSize valueOf(String str) {
			if (str.equals("m") || str.equals("min"))
				return Sizes.MINIMUM;
			if (str.equals("p") || str.equals("pref"))
				return Sizes.PREFERRED;
			if (str.equals("d") || str.equals("default"))
				return Sizes.DEFAULT;
			else
				return null;
		}

		public int maximumSize(Container container, java.util.List components,
				FormLayout.Measure minMeasure, FormLayout.Measure prefMeasure,
				FormLayout.Measure defaultMeasure) {
			FormLayout.Measure measure = this != Sizes.MINIMUM ? this != Sizes.PREFERRED ? defaultMeasure
					: prefMeasure
					: minMeasure;
			int maximum = 0;
			for (Iterator i = components.iterator(); i.hasNext();) {
				Component c = (Component) i.next();
				maximum = Math.max(maximum, measure.sizeOf(c));
			}

			return maximum;
		}

		public boolean compressible() {
			return this == Sizes.DEFAULT;
		}

		public String toString() {
			return encode();
		}

		public String encode() {
			return name.substring(0, 1);
		}

		private Object readResolve() {
			return Sizes.VALUES[ordinal];
		}

		private final transient String name;
		private static int nextOrdinal = 0;
		private final int ordinal;

		private ComponentSize(String name) {
			ordinal = nextOrdinal++;
			this.name = name;
		}

	}

	private Sizes() {
	}

	public static ConstantSize constant(String encodedValueAndUnit,
			boolean horizontal) {
		String lowerCase = encodedValueAndUnit.toLowerCase(Locale.ENGLISH);
		String trimmed = lowerCase.trim();
		return ConstantSize.valueOf(trimmed, horizontal);
	}

	public static ConstantSize dluX(int value) {
		return ConstantSize.dluX(value);
	}

	public static ConstantSize dluY(int value) {
		return ConstantSize.dluY(value);
	}

	public static ConstantSize pixel(int value) {
		return new ConstantSize(value, ConstantSize.PIXEL);
	}

	public static Size bounded(Size basis, Size lowerBound, Size upperBound) {
		return new BoundedSize(basis, lowerBound, upperBound);
	}

	public static int inchAsPixel(double in, Component component) {
		return in != 0.0D ? getUnitConverter().inchAsPixel(in, component) : 0;
	}

	public static int millimeterAsPixel(double mm, Component component) {
		return mm != 0.0D ? getUnitConverter().millimeterAsPixel(mm, component)
				: 0;
	}

	public static int centimeterAsPixel(double cm, Component component) {
		return cm != 0.0D ? getUnitConverter().centimeterAsPixel(cm, component)
				: 0;
	}

	public static int pointAsPixel(int pt, Component component) {
		return pt != 0 ? getUnitConverter().pointAsPixel(pt, component) : 0;
	}

	public static int dialogUnitXAsPixel(int dluX, Component component) {
		return dluX != 0 ? getUnitConverter().dialogUnitXAsPixel(dluX,
				component) : 0;
	}

	public static int dialogUnitYAsPixel(int dluY, Component component) {
		return dluY != 0 ? getUnitConverter().dialogUnitYAsPixel(dluY,
				component) : 0;
	}

	public static UnitConverter getUnitConverter() {
		if (unitConverter == null)
			unitConverter = DefaultUnitConverter.getInstance();
		return unitConverter;
	}

	public static void setUnitConverter(UnitConverter newUnitConverter) {
		unitConverter = newUnitConverter;
	}

	public static ConstantSize.Unit getDefaultUnit() {
		return defaultUnit;
	}

	public static void setDefaultUnit(ConstantSize.Unit unit) {
		if (unit == ConstantSize.DLUX || unit == ConstantSize.DLUY) {
			throw new IllegalArgumentException(
					"The unit must not be DLUX or DLUY. To use DLU as default unit, invoke this method with null.");
		} else {
			defaultUnit = unit;
			return;
		}
	}

	public static final ConstantSize ZERO = pixel(0);
	public static final ConstantSize DLUX1 = dluX(1);
	public static final ConstantSize DLUX2 = dluX(2);
	public static final ConstantSize DLUX3 = dluX(3);
	public static final ConstantSize DLUX4 = dluX(4);
	public static final ConstantSize DLUX5 = dluX(5);
	public static final ConstantSize DLUX6 = dluX(6);
	public static final ConstantSize DLUX7 = dluX(7);
	public static final ConstantSize DLUX8 = dluX(8);
	public static final ConstantSize DLUX9 = dluX(9);
	public static final ConstantSize DLUX11 = dluX(11);
	public static final ConstantSize DLUX14 = dluX(14);
	public static final ConstantSize DLUX21 = dluX(21);
	public static final ConstantSize DLUY1 = dluY(1);
	public static final ConstantSize DLUY2 = dluY(2);
	public static final ConstantSize DLUY3 = dluY(3);
	public static final ConstantSize DLUY4 = dluY(4);
	public static final ConstantSize DLUY5 = dluY(5);
	public static final ConstantSize DLUY6 = dluY(6);
	public static final ConstantSize DLUY7 = dluY(7);
	public static final ConstantSize DLUY8 = dluY(8);
	public static final ConstantSize DLUY9 = dluY(9);
	public static final ConstantSize DLUY11 = dluY(11);
	public static final ConstantSize DLUY14 = dluY(14);
	public static final ConstantSize DLUY21 = dluY(21);
	public static final ComponentSize MINIMUM;
	public static final ComponentSize PREFERRED;
	public static final ComponentSize DEFAULT;
	private static final ComponentSize VALUES[];
	private static UnitConverter unitConverter;
	private static ConstantSize.Unit defaultUnit;

	static {
		MINIMUM = new ComponentSize("minimum");
		PREFERRED = new ComponentSize("preferred");
		DEFAULT = new ComponentSize("default");
		VALUES = (new ComponentSize[] { MINIMUM, PREFERRED, DEFAULT });
		defaultUnit = ConstantSize.PIXEL;
	}

}

/*
 * DECOMPILATION REPORT
 * 
 * Decompiled from: D:\workspaces\MyEclipse
 * 8.6\IBMDataMovementTool\lib\IBMDataMovementTool.jar Total time: 16 ms Jad
 * reported messages/errors: Exit status: 0 Caught exceptions:
 */