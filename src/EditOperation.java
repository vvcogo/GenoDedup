package utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Objects;

public class EditOperation implements Serializable {
	private static final long serialVersionUID = 3866115109287886243L;
	public EditType type;
	public boolean isDelta = false;
	public int rPos = -1;
	public int qPos = -1;
	public int n = 1;
	public char[] c = new char[1];

	/**
	 * Constructor for n_insert_1, n_replace_1, n_delete_1, d_delete_n
	 */
	public EditOperation(EditType theType, int theRPos, int theQPos) {
		this.type = theType;
		this.rPos = theRPos;
		if (theType == EditType.DELETE_1 || theType == EditType.REPLACE_1 || theType == EditType.INSERT_1) {
			this.qPos = theQPos;
		} else {
			this.isDelta = true;
			this.n = theQPos;
		}
	}

	/**
	 * Constructor for d_insert_1, d_replace_1, d_replace_n, d_insert_n
	 */
	public EditOperation(EditType theType, int theRPos, char[] theC) {
		this.isDelta = true;
		this.type = theType;
		this.rPos = theRPos;
		this.n = theC.length;
		this.c = theC;
	}

	/**
	 * Constructor for d_delete_1
	 */
	public EditOperation(EditType theType, int theRPos) {
		this.isDelta = true;
		this.type = theType;
		this.rPos = theRPos;
	}

	/**
	 * Constructor for end_edit
	 */
	public EditOperation(EditType theType) {
		this.type = theType;
		this.rPos = 0;
		this.qPos = 0;
	}

	public int getSizeBits() {
		switch (type) {
		case NO_EDIT:
			return 3;
		case END_EDIT:
			return 3;
		case INSERT_1:
		case REPLACE_1:
			return 17;
		case DELETE_1:
			return 9;
		case INSERT_N:
		case REPLACE_N:
			return 14 + 8 * n;
		case DELETE_N:
			return 14;
		default:
			return 0;
		}
	}

	public static int getSizeBits(String theType, int theN, boolean isDna) {
		if (theN <= 0) {
			return -1;
		}
		switch (theType) {
		case "NO_EDIT":
			return 3;
		case "END_EDIT":
			return 3;
		case "INSERT":
		case "REPLACE":
			if (theN == 1) {
				if (isDna) {
					return 12;
				} else {
					return 17;
				}
			} else {
				if (isDna) {
					return 14 + 3 * theN;
				} else {
					return 14 + 8 * theN;
				}
			}
		case "DELETE":
			if (theN == 1) {
				return 9;
			} else {
				return 14;
			}
		default:
			return -1;
		}
	}

	/**
	 * EditOperation -> toString
	 */
	public String toString() {
		if (isDelta) {
			switch (type) {
			case NO_EDIT:
				return "(" + type + ")";
			case END_EDIT:
				return "(" + type + ")";
			case INSERT_1:
			case REPLACE_1:
				return "(" + type + "," + rPos + "," + (char) c[0] + ")";
			case DELETE_1:
				return "(" + type + "," + rPos + ")";
			case INSERT_N:
			case REPLACE_N:
				return "(" + type + "," + rPos + "," + n + "," + new String(c) + ")";
			case DELETE_N:
				return "(" + type + "," + rPos + "," + n + ")";
			default:
				return "";
			}
		} else {
			return "(" + type + "," + rPos + "," + qPos + ")";
		}
	}

	@Override
	public int hashCode() {
		StringBuilder sb = new StringBuilder();
		sb.append(type.toString());
		sb.append(rPos);
		sb.append(qPos);
		sb.append(n);
		sb.append(c);
		return sb.toString().hashCode();
	}
	
	

}
