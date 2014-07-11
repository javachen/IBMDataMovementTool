package com.jgoodies.forms.layout;

import java.awt.Container;
import java.io.Serializable;
import java.util.Locale;
import java.util.regex.Pattern;

import com.jgoodies.forms.util.FormUtils;

public abstract class FormSpec implements Serializable {
	public static final class DefaultAlignment implements Serializable {

		private static DefaultAlignment valueOf(String str, boolean isHorizontal) {
			if (str.equals("f") || str.equals("fill"))
				return FormSpec.FILL_ALIGN;
			if (str.equals("c") || str.equals("center"))
				return FormSpec.CENTER_ALIGN;
			if (isHorizontal) {
				if (str.equals("r") || str.equals("right"))
					return FormSpec.RIGHT_ALIGN;
				if (str.equals("l") || str.equals("left"))
					return FormSpec.LEFT_ALIGN;
				else
					return null;
			}
			if (str.equals("t") || str.equals("top"))
				return FormSpec.TOP_ALIGN;
			if (str.equals("b") || str.equals("bottom"))
				return FormSpec.BOTTOM_ALIGN;
			else
				return null;
		}

		public String toString() {
			return name;
		}

		public char abbreviation() {
			return name.charAt(0);
		}

		private Object readResolve() {
			return FormSpec.VALUES[ordinal];
		}

		private final transient String name;
		private static int nextOrdinal = 0;
		private final int ordinal;

		private DefaultAlignment(String name) {
			ordinal = nextOrdinal++;
			this.name = name;
		}

	}

	protected FormSpec(DefaultAlignment defaultAlignment, Size size,
			double resizeWeight) {
		if (size == null)
			throw new NullPointerException("The size must not be null.");
		this.defaultAlignment = defaultAlignment;
		this.size = size;
		this.resizeWeight = resizeWeight;
		if (resizeWeight < 0.0D)
			throw new IllegalArgumentException(
					"The resize weight must be non-negative.");
		else
			return;
	}

	protected FormSpec(DefaultAlignment defaultAlignment,
			String encodedDescription) {
		this(defaultAlignment, ((Size) (Sizes.DEFAULT)), 0.0D);
		parseAndInitValues(encodedDescription.toLowerCase(Locale.ENGLISH));
	}

	public final DefaultAlignment getDefaultAlignment() {
		return defaultAlignment;
	}

	public final Size getSize() {
		return size;
	}

	public final double getResizeWeight() {
		return resizeWeight;
	}

	final boolean canGrow() {
		return getResizeWeight() != 0.0D;
	}

	abstract boolean isHorizontal();

	void setDefaultAlignment(DefaultAlignment defaultAlignment) {
		this.defaultAlignment = defaultAlignment;
	}

	void setSize(Size size) {
		this.size = size;
	}

	void setResizeWeight(double resizeWeight) {
		this.resizeWeight = resizeWeight;
	}

	private void parseAndInitValues(String encodedDescription) {
		FormUtils.assertNotBlank(encodedDescription,
				"encoded form specification");
		String token[] = TOKEN_SEPARATOR_PATTERN.split(encodedDescription);
		if (token.length == 0)
			throw new IllegalArgumentException(
					"The form spec must not be empty.");
		int nextIndex = 0;
		String next = token[nextIndex++];
		DefaultAlignment alignment = DefaultAlignment.valueOf(next,
				isHorizontal());
		if (alignment != null) {
			setDefaultAlignment(alignment);
			if (token.length == 1)
				throw new IllegalArgumentException(
						"The form spec must provide a size.");
			next = token[nextIndex++];
		}
		setSize(parseSize(next));
		if (nextIndex < token.length)
			setResizeWeight(parseResizeWeight(token[nextIndex]));
	}

	private Size parseSize(String token) {
		if (token.startsWith("[") && token.endsWith("]"))
			return parseBoundedSize(token);
		if (token.startsWith("max(") && token.endsWith(")"))
			return parseOldBoundedSize(token, false);
		if (token.startsWith("min(") && token.endsWith(")"))
			return parseOldBoundedSize(token, true);
		else
			return parseAtomicSize(token);
	}

	private Size parseBoundedSize(String token) {
		String content = token.substring(1, token.length() - 1);
		String subtoken[] = BOUNDS_SEPARATOR_PATTERN.split(content);
		Size basis = null;
		Size lower = null;
		Size upper = null;
		if (subtoken.length == 2) {
			Size size1 = parseAtomicSize(subtoken[0]);
			Size size2 = parseAtomicSize(subtoken[1]);
			if (isConstant(size1)) {
				lower = size1;
				basis = size2;
			} else {
				basis = size1;
				upper = size2;
			}
		} else if (subtoken.length == 3) {
			lower = parseAtomicSize(subtoken[0]);
			basis = parseAtomicSize(subtoken[1]);
			upper = parseAtomicSize(subtoken[2]);
		}
		if ((basis instanceof Sizes.ComponentSize)
				&& (lower == null || isConstant(lower))
				&& (upper == null || isConstant(upper)))
			return new BoundedSize(basis, lower, upper);
		else
			throw new IllegalArgumentException(
					(new StringBuilder())
							.append("Illegal bounded size '")
							.append(token)
							.append("'. Must be one of:")
							.append(
									"\n[<constant size>,<logical size>]                 // lower bound")
							.append(
									"\n[<logical size>,<constant size>]                 // upper bound")
							.append(
									"\n[<constant size>,<logical size>,<constant size>] // lower and upper bound.")
							.append("\nExamples:")
							.append(
									"\n[50dlu,pref]                                     // lower bound")
							.append(
									"\n[pref,200dlu]                                    // upper bound")
							.append(
									"\n[50dlu,pref,200dlu]                              // lower and upper bound.")
							.toString());
	}

	private Size parseOldBoundedSize(String token, boolean setMax) {
		int semicolonIndex = token.indexOf(';');
		String sizeToken1 = token.substring(4, semicolonIndex);
		String sizeToken2 = token.substring(semicolonIndex + 1,
				token.length() - 1);
		Size size1 = parseAtomicSize(sizeToken1);
		Size size2 = parseAtomicSize(sizeToken2);
		if (isConstant(size1))
			if (size2 instanceof Sizes.ComponentSize)
				return new BoundedSize(size2, setMax ? null : size1,
						setMax ? size1 : null);
			else
				throw new IllegalArgumentException(
						"Bounded sizes must not be both constants.");
		if (isConstant(size2))
			return new BoundedSize(size1, setMax ? null : size2, setMax ? size2
					: null);
		else
			throw new IllegalArgumentException(
					"Bounded sizes must not be both logical.");
	}

	private Size parseAtomicSize(String token) {
		String trimmedToken = token.trim();
		if (trimmedToken.startsWith("'") && trimmedToken.endsWith("'")) {
			int length = trimmedToken.length();
			if (length < 2)
				throw new IllegalArgumentException(
						"Missing closing \"'\" for prototype.");
			else
				return new PrototypeSize(trimmedToken.substring(1, length - 1));
		}
		Sizes.ComponentSize componentSize = Sizes.ComponentSize
				.valueOf(trimmedToken);
		if (componentSize != null)
			return componentSize;
		else
			return ConstantSize.valueOf(trimmedToken, isHorizontal());
	}

	private static double parseResizeWeight(String token) {
		if (token.equals("g") || token.equals("grow"))
			return 1.0D;
		if (token.equals("n") || token.equals("nogrow") || token.equals("none"))
			return 0.0D;
		if ((token.startsWith("grow(") || token.startsWith("g("))
				&& token.endsWith(")")) {
			int leftParen = token.indexOf('(');
			int rightParen = token.indexOf(')');
			String substring = token.substring(leftParen + 1, rightParen);
			return Double.parseDouble(substring);
		} else {
			throw new IllegalArgumentException(
					(new StringBuilder())
							.append("The resize argument '")
							.append(token)
							.append("' is invalid. ")
							.append(
									" Must be one of: grow, g, none, n, grow(<double>), g(<double>)")
							.toString());
		}
	}

	private static boolean isConstant(Size aSize) {
		return (aSize instanceof ConstantSize)
				|| (aSize instanceof PrototypeSize);
	}

	public final String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append(defaultAlignment);
		buffer.append(":");
		buffer.append(size.toString());
		buffer.append(':');
		if (resizeWeight == 0.0D)
			buffer.append("noGrow");
		else if (resizeWeight == 1.0D) {
			buffer.append("grow");
		} else {
			buffer.append("grow(");
			buffer.append(resizeWeight);
			buffer.append(')');
		}
		return buffer.toString();
	}

	public final String toShortString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append(defaultAlignment.abbreviation());
		buffer.append(":");
		buffer.append(size.toString());
		buffer.append(':');
		if (resizeWeight == 0.0D)
			buffer.append("n");
		else if (resizeWeight == 1.0D) {
			buffer.append("g");
		} else {
			buffer.append("g(");
			buffer.append(resizeWeight);
			buffer.append(')');
		}
		return buffer.toString();
	}

	public final String encode() {
		StringBuffer buffer = new StringBuffer();
		DefaultAlignment alignmentDefault = isHorizontal() ? ColumnSpec.DEFAULT
				: RowSpec.DEFAULT;
		if (!alignmentDefault.equals(defaultAlignment)) {
			buffer.append(defaultAlignment.abbreviation());
			buffer.append(":");
		}
		buffer.append(size.encode());
		if (resizeWeight != 0.0D)
			if (resizeWeight == 1.0D) {
				buffer.append(':');
				buffer.append("g");
			} else {
				buffer.append(':');
				buffer.append("g(");
				buffer.append(resizeWeight);
				buffer.append(')');
			}
		return buffer.toString();
	}

	final int maximumSize(Container container, java.util.List components,
			FormLayout.Measure minMeasure, FormLayout.Measure prefMeasure,
			FormLayout.Measure defaultMeasure) {
		return size.maximumSize(container, components, minMeasure, prefMeasure,
				defaultMeasure);
	}

	static final DefaultAlignment LEFT_ALIGN;
	static final DefaultAlignment RIGHT_ALIGN;
	static final DefaultAlignment TOP_ALIGN;
	static final DefaultAlignment BOTTOM_ALIGN;
	static final DefaultAlignment CENTER_ALIGN;
	static final DefaultAlignment FILL_ALIGN;
	private static final DefaultAlignment VALUES[];
	public static final double NO_GROW = 0D;
	public static final double DEFAULT_GROW = 1D;
	private static final Pattern TOKEN_SEPARATOR_PATTERN = Pattern.compile(":");
	private static final Pattern BOUNDS_SEPARATOR_PATTERN = Pattern
			.compile("\\s*,\\s*");
	private DefaultAlignment defaultAlignment;
	private Size size;
	private double resizeWeight;

	static {
		LEFT_ALIGN = new DefaultAlignment("left");
		RIGHT_ALIGN = new DefaultAlignment("right");
		TOP_ALIGN = new DefaultAlignment("top");
		BOTTOM_ALIGN = new DefaultAlignment("bottom");
		CENTER_ALIGN = new DefaultAlignment("center");
		FILL_ALIGN = new DefaultAlignment("fill");
		VALUES = (new DefaultAlignment[] { LEFT_ALIGN, RIGHT_ALIGN, TOP_ALIGN,
				BOTTOM_ALIGN, CENTER_ALIGN, FILL_ALIGN });
	}

}

/*
 * DECOMPILATION REPORT
 * 
 * Decompiled from: D:\workspaces\MyEclipse
 * 8.6\IBMDataMovementTool\lib\IBMDataMovementTool.jar Total time: 16 ms Jad
 * reported messages/errors: Exit status: 0 Caught exceptions:
 */