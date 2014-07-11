package com.jgoodies.forms.layout;

import java.awt.Component;
import java.awt.Insets;
import java.awt.Rectangle;
import java.io.Serializable;
import java.util.Locale;
import java.util.StringTokenizer;

public final class CellConstraints implements Cloneable, Serializable {
	public static final class Alignment implements Serializable {

		static Alignment valueOf(String nameOrAbbreviation) {
			String str = nameOrAbbreviation.toLowerCase(Locale.ENGLISH);
			if (str.equals("d") || str.equals("default"))
				return CellConstraints.DEFAULT;
			if (str.equals("f") || str.equals("fill"))
				return CellConstraints.FILL;
			if (str.equals("c") || str.equals("center"))
				return CellConstraints.CENTER;
			if (str.equals("l") || str.equals("left"))
				return CellConstraints.LEFT;
			if (str.equals("r") || str.equals("right"))
				return CellConstraints.RIGHT;
			if (str.equals("t") || str.equals("top"))
				return CellConstraints.TOP;
			if (str.equals("b") || str.equals("bottom"))
				return CellConstraints.BOTTOM;
			else
				throw new IllegalArgumentException(
						(new StringBuilder())
								.append("Invalid alignment ")
								.append(nameOrAbbreviation)
								.append(
										". Must be one of: left, center, right, top, bottom, ")
								.append("fill, default, l, c, r, t, b, f, d.")
								.toString());
		}

		public String toString() {
			return name;
		}

		public char abbreviation() {
			return name.charAt(0);
		}

		private boolean isHorizontal() {
			return orientation != 1;
		}

		private boolean isVertical() {
			return orientation != 0;
		}

		private Object readResolve() {
			return CellConstraints.VALUES[ordinal];
		}

		private static final int HORIZONTAL = 0;
		private static final int VERTICAL = 1;
		private static final int BOTH = 2;
		private final transient String name;
		private final transient int orientation;
		private static int nextOrdinal = 0;
		private final int ordinal;

		private Alignment(String name, int orientation) {
			ordinal = nextOrdinal++;
			this.name = name;
			this.orientation = orientation;
		}

	}

	public CellConstraints() {
		this(1, 1);
	}

	public CellConstraints(int gridX, int gridY) {
		this(gridX, gridY, 1, 1);
	}

	public CellConstraints(int gridX, int gridY, Alignment hAlign,
			Alignment vAlign) {
		this(gridX, gridY, 1, 1, hAlign, vAlign, EMPTY_INSETS);
	}

	public CellConstraints(int gridX, int gridY, int gridWidth, int gridHeight) {
		this(gridX, gridY, gridWidth, gridHeight, DEFAULT, DEFAULT);
	}

	public CellConstraints(int gridX, int gridY, int gridWidth, int gridHeight,
			Alignment hAlign, Alignment vAlign) {
		this(gridX, gridY, gridWidth, gridHeight, hAlign, vAlign, EMPTY_INSETS);
	}

	public CellConstraints(int gridX, int gridY, int gridWidth, int gridHeight,
			Alignment hAlign, Alignment vAlign, Insets insets) {
		this.gridX = gridX;
		this.gridY = gridY;
		this.gridWidth = gridWidth;
		this.gridHeight = gridHeight;
		this.hAlign = hAlign;
		this.vAlign = vAlign;
		this.insets = insets;
		if (gridX <= 0)
			throw new IndexOutOfBoundsException(
					"The grid x must be a positive number.");
		if (gridY <= 0)
			throw new IndexOutOfBoundsException(
					"The grid y must be a positive number.");
		if (gridWidth <= 0)
			throw new IndexOutOfBoundsException(
					"The grid width must be a positive number.");
		if (gridHeight <= 0)
			throw new IndexOutOfBoundsException(
					"The grid height must be a positive number.");
		if (hAlign == null)
			throw new NullPointerException(
					"The horizontal alignment must not be null.");
		if (vAlign == null) {
			throw new NullPointerException(
					"The vertical alignment must not be null.");
		} else {
			ensureValidOrientations(hAlign, vAlign);
			return;
		}
	}

	public CellConstraints(String encodedConstraints) {
		this();
		initFromConstraints(encodedConstraints);
	}

	public CellConstraints xy(int col, int row) {
		return xywh(col, row, 1, 1);
	}

	public CellConstraints xy(int col, int row, String encodedAlignments) {
		return xywh(col, row, 1, 1, encodedAlignments);
	}

	public CellConstraints xy(int col, int row, Alignment colAlign,
			Alignment rowAlign) {
		return xywh(col, row, 1, 1, colAlign, rowAlign);
	}

	public CellConstraints xyw(int col, int row, int colSpan) {
		return xywh(col, row, colSpan, 1, DEFAULT, DEFAULT);
	}

	public CellConstraints xyw(int col, int row, int colSpan,
			String encodedAlignments) {
		return xywh(col, row, colSpan, 1, encodedAlignments);
	}

	public CellConstraints xyw(int col, int row, int colSpan,
			Alignment colAlign, Alignment rowAlign) {
		return xywh(col, row, colSpan, 1, colAlign, rowAlign);
	}

	public CellConstraints xywh(int col, int row, int colSpan, int rowSpan) {
		return xywh(col, row, colSpan, rowSpan, DEFAULT, DEFAULT);
	}

	public CellConstraints xywh(int col, int row, int colSpan, int rowSpan,
			String encodedAlignments) {
		CellConstraints result = xywh(col, row, colSpan, rowSpan);
		result.setAlignments(encodedAlignments, true);
		return result;
	}

	public CellConstraints xywh(int col, int row, int colSpan, int rowSpan,
			Alignment colAlign, Alignment rowAlign) {
		gridX = col;
		gridY = row;
		gridWidth = colSpan;
		gridHeight = rowSpan;
		hAlign = colAlign;
		vAlign = rowAlign;
		ensureValidOrientations(hAlign, vAlign);
		return this;
	}

	public CellConstraints rc(int row, int col) {
		return rchw(row, col, 1, 1);
	}

	public CellConstraints rc(int row, int col, String encodedAlignments) {
		return rchw(row, col, 1, 1, encodedAlignments);
	}

	public CellConstraints rc(int row, int col, Alignment rowAlign,
			Alignment colAlign) {
		return rchw(row, col, 1, 1, rowAlign, colAlign);
	}

	public CellConstraints rcw(int row, int col, int colSpan) {
		return rchw(row, col, 1, colSpan, DEFAULT, DEFAULT);
	}

	public CellConstraints rcw(int row, int col, int colSpan,
			String encodedAlignments) {
		return rchw(row, col, 1, colSpan, encodedAlignments);
	}

	public CellConstraints rcw(int row, int col, int colSpan,
			Alignment rowAlign, Alignment colAlign) {
		return rchw(row, col, 1, colSpan, rowAlign, colAlign);
	}

	public CellConstraints rchw(int row, int col, int rowSpan, int colSpan) {
		return rchw(row, col, rowSpan, colSpan, DEFAULT, DEFAULT);
	}

	public CellConstraints rchw(int row, int col, int rowSpan, int colSpan,
			String encodedAlignments) {
		CellConstraints result = rchw(row, col, rowSpan, colSpan);
		result.setAlignments(encodedAlignments, false);
		return result;
	}

	public CellConstraints rchw(int row, int col, int rowSpan, int colSpan,
			Alignment rowAlign, Alignment colAlign) {
		return xywh(col, row, colSpan, rowSpan, colAlign, rowAlign);
	}

	private void initFromConstraints(String encodedConstraints) {
		StringTokenizer tokenizer = new StringTokenizer(encodedConstraints,
				" ,");
		int argCount = tokenizer.countTokens();
		if (argCount != 2 && argCount != 4 && argCount != 6)
			throw new IllegalArgumentException(
					"You must provide 2, 4 or 6 arguments.");
		Integer nextInt = decodeInt(tokenizer.nextToken());
		if (nextInt == null)
			throw new IllegalArgumentException(
					"First cell constraint element must be a number.");
		gridX = nextInt.intValue();
		if (gridX <= 0)
			throw new IndexOutOfBoundsException(
					"The grid x must be a positive number.");
		nextInt = decodeInt(tokenizer.nextToken());
		if (nextInt == null)
			throw new IllegalArgumentException(
					"Second cell constraint element must be a number.");
		gridY = nextInt.intValue();
		if (gridY <= 0)
			throw new IndexOutOfBoundsException(
					"The grid y must be a positive number.");
		if (!tokenizer.hasMoreTokens())
			return;
		String token = tokenizer.nextToken();
		nextInt = decodeInt(token);
		if (nextInt != null) {
			gridWidth = nextInt.intValue();
			if (gridWidth <= 0)
				throw new IndexOutOfBoundsException(
						"The grid width must be a positive number.");
			nextInt = decodeInt(tokenizer.nextToken());
			if (nextInt == null)
				throw new IllegalArgumentException(
						"Fourth cell constraint element must be like third.");
			gridHeight = nextInt.intValue();
			if (gridHeight <= 0)
				throw new IndexOutOfBoundsException(
						"The grid height must be a positive number.");
			if (!tokenizer.hasMoreTokens())
				return;
			token = tokenizer.nextToken();
		}
		hAlign = decodeAlignment(token);
		vAlign = decodeAlignment(tokenizer.nextToken());
		ensureValidOrientations(hAlign, vAlign);
	}

	private void setAlignments(String encodedAlignments,
			boolean horizontalThenVertical) {
		StringTokenizer tokenizer = new StringTokenizer(encodedAlignments, " ,");
		Alignment first = decodeAlignment(tokenizer.nextToken());
		Alignment second = decodeAlignment(tokenizer.nextToken());
		hAlign = horizontalThenVertical ? first : second;
		vAlign = horizontalThenVertical ? second : first;
		ensureValidOrientations(hAlign, vAlign);
	}

	private Integer decodeInt(String token) {
		try {
			return Integer.decode(token);
		} catch (NumberFormatException e) {
			return null;
		}
	}

	private Alignment decodeAlignment(String encodedAlignment) {
		return Alignment.valueOf(encodedAlignment);
	}

	void ensureValidGridBounds(int colCount, int rowCount) {
		if (gridX <= 0)
			throw new IndexOutOfBoundsException((new StringBuilder()).append(
					"The column index ").append(gridX).append(
					" must be positive.").toString());
		if (gridX > colCount)
			throw new IndexOutOfBoundsException((new StringBuilder()).append(
					"The column index ").append(gridX).append(
					" must be less than or equal to ").append(colCount).append(
					".").toString());
		if ((gridX + gridWidth) - 1 > colCount)
			throw new IndexOutOfBoundsException((new StringBuilder()).append(
					"The grid width ").append(gridWidth).append(
					" must be less than or equal to ").append(
					(colCount - gridX) + 1).append(".").toString());
		if (gridY <= 0)
			throw new IndexOutOfBoundsException((new StringBuilder()).append(
					"The row index ").append(gridY)
					.append(" must be positive.").toString());
		if (gridY > rowCount)
			throw new IndexOutOfBoundsException((new StringBuilder()).append(
					"The row index ").append(gridY).append(
					" must be less than or equal to ").append(rowCount).append(
					".").toString());
		if ((gridY + gridHeight) - 1 > rowCount)
			throw new IndexOutOfBoundsException((new StringBuilder()).append(
					"The grid height ").append(gridHeight).append(
					" must be less than or equal to ").append(
					(rowCount - gridY) + 1).append(".").toString());
		else
			return;
	}

	private void ensureValidOrientations(Alignment horizontalAlignment,
			Alignment verticalAlignment) {
		if (!horizontalAlignment.isHorizontal())
			throw new IllegalArgumentException(
					"The horizontal alignment must be one of: left, center, right, fill, default.");
		if (!verticalAlignment.isVertical())
			throw new IllegalArgumentException(
					"The vertical alignment must be one of: top, center, botto, fill, default.");
		else
			return;
	}

	void setBounds(Component c, FormLayout layout, Rectangle cellBounds,
			FormLayout.Measure minWidthMeasure,
			FormLayout.Measure minHeightMeasure,
			FormLayout.Measure prefWidthMeasure,
			FormLayout.Measure prefHeightMeasure) {
		ColumnSpec colSpec = gridWidth != 1 ? null : layout
				.getColumnSpec(gridX);
		RowSpec rowSpec = gridHeight != 1 ? null : layout.getRowSpec(gridY);
		Alignment concreteHAlign = concreteAlignment(hAlign, colSpec);
		Alignment concreteVAlign = concreteAlignment(vAlign, rowSpec);
		Insets concreteInsets = insets == null ? EMPTY_INSETS : insets;
		int cellX = cellBounds.x + concreteInsets.left;
		int cellY = cellBounds.y + concreteInsets.top;
		int cellW = cellBounds.width - concreteInsets.left
				- concreteInsets.right;
		int cellH = cellBounds.height - concreteInsets.top
				- concreteInsets.bottom;
		int compW = componentSize(c, colSpec, cellW, minWidthMeasure,
				prefWidthMeasure);
		int compH = componentSize(c, rowSpec, cellH, minHeightMeasure,
				prefHeightMeasure);
		int x = origin(concreteHAlign, cellX, cellW, compW);
		int y = origin(concreteVAlign, cellY, cellH, compH);
		int w = extent(concreteHAlign, cellW, compW);
		int h = extent(concreteVAlign, cellH, compH);
		c.setBounds(x, y, w, h);
	}

	private Alignment concreteAlignment(Alignment cellAlignment,
			FormSpec formSpec) {
		return formSpec != null ? usedAlignment(cellAlignment, formSpec)
				: cellAlignment != DEFAULT ? cellAlignment : FILL;
	}

	private Alignment usedAlignment(Alignment cellAlignment, FormSpec formSpec) {
		if (cellAlignment != DEFAULT)
			return cellAlignment;
		FormSpec.DefaultAlignment defaultAlignment = formSpec
				.getDefaultAlignment();
		if (defaultAlignment == FormSpec.FILL_ALIGN)
			return FILL;
		if (defaultAlignment == ColumnSpec.LEFT)
			return LEFT;
		if (defaultAlignment == FormSpec.CENTER_ALIGN)
			return CENTER;
		if (defaultAlignment == ColumnSpec.RIGHT)
			return RIGHT;
		if (defaultAlignment == RowSpec.TOP)
			return TOP;
		else
			return BOTTOM;
	}

	private int componentSize(Component component, FormSpec formSpec,
			int cellSize, FormLayout.Measure minMeasure,
			FormLayout.Measure prefMeasure) {
		if (formSpec == null)
			return prefMeasure.sizeOf(component);
		if (formSpec.getSize() == Sizes.MINIMUM)
			return minMeasure.sizeOf(component);
		if (formSpec.getSize() == Sizes.PREFERRED)
			return prefMeasure.sizeOf(component);
		else
			return Math.min(cellSize, prefMeasure.sizeOf(component));
	}

	private int origin(Alignment alignment, int cellOrigin, int cellSize,
			int componentSize) {
		if (alignment == RIGHT || alignment == BOTTOM)
			return (cellOrigin + cellSize) - componentSize;
		if (alignment == CENTER)
			return cellOrigin + (cellSize - componentSize) / 2;
		else
			return cellOrigin;
	}

	private int extent(Alignment alignment, int cellSize, int componentSize) {
		return alignment != FILL ? componentSize : cellSize;
	}

	public Object clone() {
		try {
			CellConstraints c = (CellConstraints) super.clone();
			c.insets = (Insets) insets.clone();
			return c;
		} catch (CloneNotSupportedException e) {
			throw new InternalError();
		}
	}

	public String toString() {
		StringBuffer buffer = new StringBuffer("CellConstraints");
		buffer.append("[x=");
		buffer.append(gridX);
		buffer.append("; y=");
		buffer.append(gridY);
		buffer.append("; w=");
		buffer.append(gridWidth);
		buffer.append("; h=");
		buffer.append(gridHeight);
		buffer.append("; hAlign=");
		buffer.append(hAlign);
		buffer.append("; vAlign=");
		buffer.append(vAlign);
		if (!EMPTY_INSETS.equals(insets)) {
			buffer.append("; insets=");
			buffer.append(insets);
		}
		buffer.append("; honorsVisibility=");
		buffer.append(honorsVisibility);
		buffer.append(']');
		return buffer.toString();
	}

	public String toShortString() {
		return toShortString(null);
	}

	public String toShortString(FormLayout layout) {
		StringBuffer buffer = new StringBuffer("(");
		buffer.append(formatInt(gridX));
		buffer.append(", ");
		buffer.append(formatInt(gridY));
		buffer.append(", ");
		buffer.append(formatInt(gridWidth));
		buffer.append(", ");
		buffer.append(formatInt(gridHeight));
		buffer.append(", \"");
		buffer.append(hAlign.abbreviation());
		if (hAlign == DEFAULT && layout != null) {
			buffer.append('=');
			ColumnSpec colSpec = gridWidth != 1 ? null : layout
					.getColumnSpec(gridX);
			buffer.append(concreteAlignment(hAlign, colSpec).abbreviation());
		}
		buffer.append(", ");
		buffer.append(vAlign.abbreviation());
		if (vAlign == DEFAULT && layout != null) {
			buffer.append('=');
			RowSpec rowSpec = gridHeight != 1 ? null : layout.getRowSpec(gridY);
			buffer.append(concreteAlignment(vAlign, rowSpec).abbreviation());
		}
		buffer.append("\"");
		if (!EMPTY_INSETS.equals(insets)) {
			buffer.append(", ");
			buffer.append(insets);
		}
		if (honorsVisibility != null)
			buffer.append(honorsVisibility.booleanValue() ? "honors visibility"
					: "ignores visibility");
		buffer.append(')');
		return buffer.toString();
	}

	private String formatInt(int number) {
		String str = Integer.toString(number);
		return number >= 10 ? str : (new StringBuilder()).append(" ").append(
				str).toString();
	}

	public static final Alignment DEFAULT;
	public static final Alignment FILL;
	public static final Alignment LEFT;
	public static final Alignment RIGHT;
	public static final Alignment CENTER;
	public static final Alignment TOP;
	public static final Alignment BOTTOM;
	private static final Alignment VALUES[];
	private static final Insets EMPTY_INSETS = new Insets(0, 0, 0, 0);
	public int gridX;
	public int gridY;
	public int gridWidth;
	public int gridHeight;
	public Alignment hAlign;
	public Alignment vAlign;
	public Insets insets;
	public Boolean honorsVisibility;

	static {
		DEFAULT = new Alignment("default", 2);
		FILL = new Alignment("fill", 2);
		LEFT = new Alignment("left", 0);
		RIGHT = new Alignment("right", 0);
		CENTER = new Alignment("center", 2);
		TOP = new Alignment("top", 1);
		BOTTOM = new Alignment("bottom", 1);
		VALUES = (new Alignment[] { DEFAULT, FILL, LEFT, RIGHT, CENTER, TOP,
				BOTTOM });
	}

}

/*
 * DECOMPILATION REPORT
 * 
 * Decompiled from: D:\workspaces\MyEclipse
 * 8.6\IBMDataMovementTool\lib\IBMDataMovementTool.jar Total time: 78 ms Jad
 * reported messages/errors: Exit status: 0 Caught exceptions:
 */