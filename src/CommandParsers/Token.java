package CommandParsers;

import java.util.ArrayList;
import java.util.List;

public class Token {

    public enum Type {
        WORD,   // keywords, identifiers, True/False/null
        STRING, // string in double quotes
        NUMBER, // int or double
        LPAREN, // (
        RPAREN, // )
        COMMA,  // ,
        STAR,   // *
        DOT,    // .
        RELOP,  // >, >=, <, <=, =, <>
        PLUS,   // +
        MINUS,  // -
        SLASH   // /
    }
    public final Type type;
    public final String value;

    public Token(Type type, String value) {
        this.type = type;
        this.value = value;
    }

    @Override
    public String toString() {
        return type + "(" + value + ")";
    }

    public static List<Token> tokenize(String input) throws Exception {
        List<Token> tokens = new ArrayList<>();
        int i = 0;

        while (i < input.length()) {
            char c = input.charAt(i);

            if (Character.isWhitespace(c)) {
                i++;
                continue;
            }

            if (c == '(') {
                tokens.add(new Token(Token.Type.LPAREN, "("));
                i++;
            } else if (c == ')') {
                tokens.add(new Token(Token.Type.RPAREN, ")"));
                i++;
            } else if (c == ',') {
                tokens.add(new Token(Token.Type.COMMA, ","));
                i++;
            } else if (c == '*') {
                tokens.add(new Token(Token.Type.STAR, "*"));
                i++;
            } else if (c == '.') {
                tokens.add(new Token(Token.Type.DOT, "."));
                i++;
            } else if (c == '+') {
                tokens.add(new Token(Token.Type.PLUS, "+"));
                i++;
            } else if (c == '-') {
                tokens.add(new Token(Token.Type.MINUS, "-"));
                i++;
            } else if (c == '/') {
                tokens.add(new Token(Token.Type.SLASH, "/"));
                i++;
            } else if (c == '=') {
                tokens.add(new Token(Token.Type.RELOP, "="));
                i++;
            } else if (c == '<') {
                if (i + 1 < input.length() && input.charAt(i + 1) == '=') {
                    tokens.add(new Token(Token.Type.RELOP, "<="));
                    i += 2;
                } else if (i + 1 < input.length() && input.charAt(i + 1) == '>') {
                    tokens.add(new Token(Token.Type.RELOP, "<>"));
                    i += 2;
                } else {
                    tokens.add(new Token(Token.Type.RELOP, "<"));
                    i++;
                }
            } else if (c == '>') {
                if (i + 1 < input.length() && input.charAt(i + 1) == '=') {
                    tokens.add(new Token(Token.Type.RELOP, ">="));
                    i += 2;
                } else {
                    tokens.add(new Token(Token.Type.RELOP, ">"));
                    i++;
                }
            } else if (c == '"') {
                i++;

                StringBuilder sb = new StringBuilder();
                while (i < input.length() && input.charAt(i) != '"') {
                    sb.append(input.charAt(i));
                    i++;
                }

                if (i >= input.length()) {
                    throw new Exception("Unterminated string literal");
                }
                i++; // closing quote

                tokens.add(new Token(Token.Type.STRING, sb.toString()));
            } else if (Character.isDigit(c)) {
                StringBuilder sb = new StringBuilder();
                boolean hasDecimal = false;

                while (i < input.length() && (Character.isDigit(input.charAt(i)) || input.charAt(i) == '.')) {
                    if (input.charAt(i) == '.') {
                        if (hasDecimal) {
                            break;
                        }
                        hasDecimal = true;
                    }
                    sb.append(input.charAt(i));
                    i++;
                }

                tokens.add(new Token(Token.Type.NUMBER, sb.toString()));
            } else if (Character.isLetterOrDigit(c)) {
                StringBuilder sb = new StringBuilder();
                while (i < input.length() && Character.isLetterOrDigit(input.charAt(i))) {
                    sb.append(input.charAt(i));
                    i++;
                }

                tokens.add(new Token(Token.Type.WORD, sb.toString()));
            } else {
                throw new Exception("Unexpected character: '" + c + "'");
            }
        }

        return tokens;
    }
}
