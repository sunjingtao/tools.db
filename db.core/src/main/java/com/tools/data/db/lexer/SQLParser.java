package com.tools.data.db.lexer;

import java.util.ArrayList;

import com.tools.data.db.lexer.querymodel.*;

public class SQLParser implements SQLParserConstants {

  public static void main(String args[]) throws ParseException {
      SQLParser parser = new SQLParser(System.in);
      QueryNode query = parser.SQLQuery();
//      System.out.println("Query generated from model: ");
//      System.out.println(query.genText());
  }

// select-clause from-clause where-clause groupby-clause having-clause orderby-clause
  final public QueryNode SQLQuery() throws ParseException {
//    System.out.println("parse query");
    SelectNode s=null;
    FromNode f=null;
    WhereNode w = null;
    OrderByNode o=null;
    GroupByNode g=null;
    HavingNode h=null;
    // Select List
        s = SQLSelect();
    // Table Expression
        // Each clause takes table from previous clause, produces table for next clause
        f = SQLFrom();
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case WHERE:
      w = SQLWhere();
      break;
    default:
      jj_la1[0] = jj_gen;
      ;
    }
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case GROUP:
      g = SQLGroupBy();
      break;
    default:
      jj_la1[1] = jj_gen;
      ;
    }
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case HAVING:
      h = SQLHaving();
      break;
    default:
      jj_la1[2] = jj_gen;
      ;
    }
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case ORDER:
      o = SQLOrderBy();
      break;
    default:
      jj_la1[3] = jj_gen;
      ;
    }
    jj_consume_token(0);
        {if (true) return new QueryNode(s, f, w, g, h, o);}
    throw new Error("Missing return statement in function");
  }

// SELECT [ quantifier ] column (, column)*
  final public SelectNode SQLSelect() throws ParseException {
    ArrayList selctItemList=new ArrayList();
    String q="ALL";
    ColumnItem item = null;
    jj_consume_token(SELECT);
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case ALL:
    case DISTINCT:
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case ALL:
        jj_consume_token(ALL);
                      q="ALL";
        break;
      case DISTINCT:
        jj_consume_token(DISTINCT);
                      q="DISTINCT";
        break;
      default:
        jj_la1[4] = jj_gen;
        jj_consume_token(-1);
        throw new ParseException();
      }
      break;
    default:
      jj_la1[5] = jj_gen;
      ;
    }
    item = SQLColumnItem();
          selctItemList.add(item);
    label_1:
    while (true) {
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case 42:
        ;
        break;
      default:
        jj_la1[6] = jj_gen;
        break label_1;
      }
      jj_consume_token(42);
      item = SQLColumnItem();
          selctItemList.add(item);
    }
        {if (true) return new SelectNode(selctItemList, q);}
    throw new Error("Missing return statement in function");
  }

  final public ColumnItem SQLColumnItem() throws ParseException {
    ColumnItem item = null;
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case AVG:
      jj_consume_token(AVG);
             item = SQLSetFunction(SetFunction.AVG);
      break;
    case COUNT:
      jj_consume_token(COUNT);
                 item = SQLSetFunction(SetFunction.COUNT);
      break;
    case MAX:
      jj_consume_token(MAX);
               item = SQLSetFunction(SetFunction.MAX);
      break;
    case MIN:
      jj_consume_token(MIN);
               item = SQLSetFunction(SetFunction.MIN);
      break;
    case SUM:
      jj_consume_token(SUM);
               item = SQLSetFunction(SetFunction.SUM);
      break;
    case ORDINARY_ID:
    case DELIMITED_ID:
    case MYSQL_DELIMITED_ID:
    case 43:
      item = SQLColumn();
      break;
    default:
      jj_la1[7] = jj_gen;
      jj_consume_token(-1);
      throw new ParseException();
    }
        {if (true) return item;}
    throw new Error("Missing return statement in function");
  }

// * | [ tablename . ] columnname
// ToDo: Add semantic processing for elided tablenames
  final public ColumnNode SQLColumn() throws ParseException {
    Identifier tableName=null, columnName=null, schemaName=null, derivedColName=null;
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case 43:
      jj_consume_token(43);
           columnName = new Identifier("*", false);
      break;
    default:
      jj_la1[8] = jj_gen;
      if (jj_2_1(4)) {
        schemaName = identifier();
        jj_consume_token(44);
        tableName = identifier();
        jj_consume_token(44);
        columnName = identifier();
      } else if (jj_2_2(2)) {
        tableName = identifier();
        jj_consume_token(44);
        columnName = identifier();
      } else {
        switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
        case ORDINARY_ID:
        case DELIMITED_ID:
        case MYSQL_DELIMITED_ID:
          columnName = identifier();
          break;
        default:
          jj_la1[9] = jj_gen;
          jj_consume_token(-1);
          throw new ParseException();
        }
      }
    }
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case AS:
    case ORDINARY_ID:
    case DELIMITED_ID:
    case MYSQL_DELIMITED_ID:
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case AS:
        jj_consume_token(AS);
        break;
      default:
        jj_la1[10] = jj_gen;
        ;
      }
      derivedColName = identifier();
      break;
    default:
      jj_la1[11] = jj_gen;
      ;
    }
        {if (true) return ColumnNode.make(tableName, columnName, schemaName, derivedColName);}
    throw new Error("Missing return statement in function");
  }

  final public SetFunction SQLSetFunction(int type) throws ParseException {
    ColumnNode c = null;
    Identifier alias = null;
    jj_consume_token(45);
    c = SQLColumn();
    jj_consume_token(46);
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case AS:
    case ORDINARY_ID:
    case DELIMITED_ID:
    case MYSQL_DELIMITED_ID:
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case AS:
        jj_consume_token(AS);
        break;
      default:
        jj_la1[12] = jj_gen;
        ;
      }
      alias = identifier();
      break;
    default:
      jj_la1[13] = jj_gen;
      ;
    }
        {if (true) return new SetFunction(type, c, alias);}
    throw new Error("Missing return statement in function");
  }

// FROM table-reference [, table-reference]
  final public FromNode SQLFrom() throws ParseException {
    ArrayList tableList = new ArrayList();
    JoinTableNode jt=null;
    jj_consume_token(FROM);
    jt = SQLFirstJoinTable();
      tableList.add(jt);
    label_2:
    while (true) {
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case CROSS:
      case FULL:
      case INNER:
      case LEFT:
      case JOIN:
      case NATURAL:
      case OUTER:
      case RIGHT:
      case 42:
        ;
        break;
      default:
        jj_la1[14] = jj_gen;
        break label_2;
      }
      jt = SQLJoinTable();
      tableList.add(jt);
    }
      {if (true) return new FromNode(tableList);}
    throw new Error("Missing return statement in function");
  }

// GROUP by grouping-column [, grouping-column ]
  final public GroupByNode SQLGroupBy() throws ParseException {
    ArrayList columnList=new ArrayList();
    ColumnItem col;
    jj_consume_token(GROUP);
    jj_consume_token(BY);
    col = SQLColumnItem();
      columnList.add(col);
    label_3:
    while (true) {
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case 42:
        ;
        break;
      default:
        jj_la1[15] = jj_gen;
        break label_3;
      }
      jj_consume_token(42);
      col = SQLColumnItem();
        columnList.add(col);
    }
        {if (true) return new GroupByNode(columnList);}
    throw new Error("Missing return statement in function");
  }

// HAVING search-condition
  final public HavingNode SQLHaving() throws ParseException {
    Expression cond;
    jj_consume_token(HAVING);
    cond = SQLOrExpr();
        {if (true) return new HavingNode(cond);}
    throw new Error("Missing return statement in function");
  }

  final public JoinTableNode SQLFirstJoinTable() throws ParseException {
    TableNode tbl;
    tbl = SQLTable();
      {if (true) return new JoinTableNode(tbl);}
    throw new Error("Missing return statement in function");
  }

  final public JoinTableNode SQLJoinTable() throws ParseException {
    String joinType=null;
    Expression cond=null;
    TableNode tbl;
    joinType = SQLJoinType();
    tbl = SQLTable();
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case ON:
      jj_consume_token(ON);
      cond = SQLOrExpr();
      break;
    default:
      jj_la1[16] = jj_gen;
      ;
    }
        {if (true) return new JoinTableNode(tbl, joinType, cond);}
    throw new Error("Missing return statement in function");
  }

// tableName [ [ AS ] corrName ]
  final public TableNode SQLTable() throws ParseException {
    Identifier tableName=null, corrName=null, schemaName=null;
    if (jj_2_3(2)) {
      schemaName = identifier();
      jj_consume_token(44);
      tableName = identifier();
    } else {
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case ORDINARY_ID:
      case DELIMITED_ID:
      case MYSQL_DELIMITED_ID:
        tableName = identifier();
        break;
      default:
        jj_la1[17] = jj_gen;
        jj_consume_token(-1);
        throw new ParseException();
      }
    }
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case AS:
    case ORDINARY_ID:
    case DELIMITED_ID:
    case MYSQL_DELIMITED_ID:
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case AS:
        jj_consume_token(AS);
        break;
      default:
        jj_la1[18] = jj_gen;
        ;
      }
      corrName = identifier();
      break;
    default:
      jj_la1[19] = jj_gen;
      ;
    }
            {if (true) return TableNode.make(tableName, corrName, schemaName);}
    throw new Error("Missing return statement in function");
  }

  final public String SQLJoinType() throws ParseException {
    String type=null;
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case 42:
      jj_consume_token(42);
              type = "CROSS";
      break;
    case CROSS:
    case FULL:
    case INNER:
    case LEFT:
    case JOIN:
    case NATURAL:
    case OUTER:
    case RIGHT:
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case CROSS:
      case FULL:
      case INNER:
      case LEFT:
      case NATURAL:
      case OUTER:
      case RIGHT:
        switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
        case CROSS:
          jj_consume_token(CROSS);
                      type = "CROSS";
          break;
        case INNER:
          jj_consume_token(INNER);
                      type = "INNER";
          break;
        case NATURAL:
          jj_consume_token(NATURAL);
                        type = "NATURAL";
          break;
        case FULL:
        case LEFT:
        case OUTER:
        case RIGHT:
          switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
          case FULL:
          case LEFT:
          case RIGHT:
            switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
            case LEFT:
              jj_consume_token(LEFT);
                       type = "LEFT OUTER";
              break;
            case RIGHT:
              jj_consume_token(RIGHT);
                        type = "RIGHT OUTER";
              break;
            case FULL:
              jj_consume_token(FULL);
                       type = "FULL OUTER";
              break;
            default:
              jj_la1[20] = jj_gen;
              jj_consume_token(-1);
              throw new ParseException();
            }
            break;
          default:
            jj_la1[21] = jj_gen;
            ;
          }
          jj_consume_token(OUTER);
                      type = "LEFT OUTER";
          break;
        default:
          jj_la1[22] = jj_gen;
          jj_consume_token(-1);
          throw new ParseException();
        }
        break;
      default:
        jj_la1[23] = jj_gen;
        ;
      }
      jj_consume_token(JOIN);
                   if (type==null) type="INNER";
      break;
    default:
      jj_la1[24] = jj_gen;
      jj_consume_token(-1);
      throw new ParseException();
    }
      {if (true) return type;}
    throw new Error("Missing return statement in function");
  }

// WHERE <Expression>
  final public WhereNode SQLWhere() throws ParseException {
    Expression o;
    jj_consume_token(WHERE);
    o = SQLOrExpr();
        {if (true) return new WhereNode(o);}
    throw new Error("Missing return statement in function");
  }

// An arbitrary search-condition, in a WHERE or ON clause
// We assume, without loss of generality, that the search-condition
// is in Disjunctive Normal Form (DNF)
// If join is true, we are in a join clause, and only allow columns as primitive

// [(] <AndExpr> [ OR <AndExpr>] [)]
  final public Expression SQLOrExpr() throws ParseException {
//    System.out.println("SQLOrExpr");
    Expression c=null;
    if (jj_2_4(2147483647)) {
      c = SQLOr();
    } else {
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case 45:
        jj_consume_token(45);
        c = SQLOrExpr();
        jj_consume_token(46);
        break;
      default:
        jj_la1[25] = jj_gen;
        jj_consume_token(-1);
        throw new ParseException();
      }
    }
        {if (true) return c;}
    throw new Error("Missing return statement in function");
  }

  final public Expression SQLOr() throws ParseException {
//    System.out.println("SQLOr");
    Expression a;
    ArrayList forms = new ArrayList();
    a = SQLAndExpr();
          forms.add(a);
    label_4:
    while (true) {
      if (jj_2_5(2)) {
        ;
      } else {
        break label_4;
      }
      jj_consume_token(OR);
      a = SQLOrExpr();
          forms.add(a);
    }
        if (forms.size()==1)
            {if (true) return a;}
        else
            {if (true) return new OrNode(forms);}
    throw new Error("Missing return statement in function");
  }

// [(] <NotExpr> [ AND <NotExpr>] [)]
  final public Expression SQLAndExpr() throws ParseException {
//    System.out.println("SQLAndExpr");
    Expression c=null;
    if (jj_2_6(2147483647)) {
      c = SQLAnd();
    } else {
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case 45:
        jj_consume_token(45);
        c = SQLOrExpr();
        jj_consume_token(46);
        break;
      default:
        jj_la1[26] = jj_gen;
        jj_consume_token(-1);
        throw new ParseException();
      }
    }
        {if (true) return c;}
    throw new Error("Missing return statement in function");
  }

  final public Expression SQLAnd() throws ParseException {
//    System.out.println("SQLAnd");
    Expression c;
    ArrayList forms = new ArrayList();
    c = SQLNot();
      forms.add(c);
    label_5:
    while (true) {
      if (jj_2_7(2)) {
        ;
      } else {
        break label_5;
      }
      jj_consume_token(AND);
      c = SQLAndExpr();
      forms.add(c);
    }
        if (forms.size()==1)
            {if (true) return c;}
        else
            {if (true) return new AndNode(forms);}
    throw new Error("Missing return statement in function");
  }

// [ NOT ] <Predicate>
  final public Expression SQLNot() throws ParseException {
//    System.out.println("SQLNot");
    Expression p;
    boolean foundNot=false;
    if (jj_2_8(2)) {
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case NOT:
        jj_consume_token(NOT);
            foundNot = true;
        break;
      default:
        jj_la1[27] = jj_gen;
        ;
      }
      jj_consume_token(45);
      p = SQLOrExpr();
      jj_consume_token(46);
    } else {
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case AVG:
      case COUNT:
      case MAX:
      case MIN:
      case NOT:
      case SUM:
      case ORDINARY_ID:
      case DELIMITED_ID:
      case MYSQL_DELIMITED_ID:
      case STRING_LITERAL:
      case INTEGER_LITERAL:
      case 43:
      case 47:
        switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
        case NOT:
          jj_consume_token(NOT);
            foundNot = true;
          break;
        default:
          jj_la1[28] = jj_gen;
          ;
        }
        p = SQLPredicate();
        break;
      default:
        jj_la1[29] = jj_gen;
        jj_consume_token(-1);
        throw new ParseException();
      }
    }
      if (!foundNot)
          {if (true) return p;}
      else
          {if (true) return new NotNode(p);}
    throw new Error("Missing return statement in function");
  }

// For the moment, we assume that the expression has the form
// Column = Column
  final public Predicate SQLPredicate() throws ParseException {
//    System.out.println("SQLPredicate");
    Value val1, val2;
    String op;
    val1 = SQLValue();
    op = relop();
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case 45:
      jj_consume_token(45);
      val2 = SQLCommaSeparatedStrings();
      jj_consume_token(46);
      break;
    case AVG:
    case COUNT:
    case MAX:
    case MIN:
    case SUM:
    case ORDINARY_ID:
    case DELIMITED_ID:
    case MYSQL_DELIMITED_ID:
    case STRING_LITERAL:
    case INTEGER_LITERAL:
    case 43:
    case 47:
      val2 = SQLValue();
      break;
    default:
      jj_la1[30] = jj_gen;
      jj_consume_token(-1);
      throw new ParseException();
    }
        {if (true) return new Predicate(val1, val2, op);}
    throw new Error("Missing return statement in function");
  }

  final public Value SQLCommaSeparatedStrings() throws ParseException {
    String lit;
    StringBuffer litBuffer = new StringBuffer();
    lit = literal();
      litBuffer.append ( lit );
    label_6:
    while (true) {
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case 42:
        ;
        break;
      default:
        jj_la1[31] = jj_gen;
        break label_6;
      }
      jj_consume_token(42);
      lit = literal();
        litBuffer.append(",");
        litBuffer.append(lit);
    }
        {if (true) return new Literal ( " ( " + litBuffer.toString() + " ) " );}
    throw new Error("Missing return statement in function");
  }

  final public Value SQLValue() throws ParseException {
    ColumnItem col;
    String lit;
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case AVG:
    case COUNT:
    case MAX:
    case MIN:
    case SUM:
    case ORDINARY_ID:
    case DELIMITED_ID:
    case MYSQL_DELIMITED_ID:
    case 43:
      /* "?"                 { return new Literal("?"); }
          | */
          col = SQLColumnItem();
                            {if (true) return col;}
      break;
    case STRING_LITERAL:
    case INTEGER_LITERAL:
    case 47:
      lit = literal();
                          {if (true) return new Literal(lit);}
      break;
    default:
      jj_la1[32] = jj_gen;
      jj_consume_token(-1);
      throw new ParseException();
    }
    throw new Error("Missing return statement in function");
  }

// ORDER by sort-specification [, sort-specification]
  final public OrderByNode SQLOrderBy() throws ParseException {
    ArrayList sortSpecificationList=new ArrayList();
    SortSpecification ss=null;
    jj_consume_token(ORDER);
    jj_consume_token(BY);
    ss = SQLSortSpecification();
      sortSpecificationList.add(ss);
    label_7:
    while (true) {
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case 42:
        ;
        break;
      default:
        jj_la1[33] = jj_gen;
        break label_7;
      }
      jj_consume_token(42);
      ss = SQLSortSpecification();
        sortSpecificationList.add(ss);
    }
        {if (true) return new OrderByNode(sortSpecificationList);}
    throw new Error("Missing return statement in function");
  }

// column [ ASC | DESC ]
  final public SortSpecification SQLSortSpecification() throws ParseException {
    ColumnItem col=null;
    String dir="ASC";
    col = SQLColumnItem();
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case ASC:
    case DESC:
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case ASC:
        jj_consume_token(ASC);
                 dir="ASC";
        break;
      case DESC:
        jj_consume_token(DESC);
                 dir="DESC";
        break;
      default:
        jj_la1[34] = jj_gen;
        jj_consume_token(-1);
        throw new ParseException();
      }
      break;
    default:
      jj_la1[35] = jj_gen;
      ;
    }
        {if (true) return new SortSpecification(col, dir);}
    throw new Error("Missing return statement in function");
  }

  final public Identifier identifier() throws ParseException {
    Token t;
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case ORDINARY_ID:
      t = jj_consume_token(ORDINARY_ID);
        {if (true) return new Identifier(t.image,false);}
      break;
    case DELIMITED_ID:
      t = jj_consume_token(DELIMITED_ID);
        {if (true) return new Identifier(t.image.substring(1,t.image.length()-1),true);}
      break;
    case MYSQL_DELIMITED_ID:
      t = jj_consume_token(MYSQL_DELIMITED_ID);
        {if (true) return new Identifier(t.image.substring(1,t.image.length()-1),true);}
      break;
    default:
      jj_la1[36] = jj_gen;
      jj_consume_token(-1);
      throw new ParseException();
    }
    throw new Error("Missing return statement in function");
  }

  final public String relop() throws ParseException {
    Token t;
    t = jj_consume_token(RELOP);
        {if (true) return t.image;}
    throw new Error("Missing return statement in function");
  }

  final public String literal() throws ParseException {
    Token t;
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case STRING_LITERAL:
      t = jj_consume_token(STRING_LITERAL);
      break;
    case INTEGER_LITERAL:
      t = jj_consume_token(INTEGER_LITERAL);
      break;
    case 47:
      t = jj_consume_token(47);
      break;
    default:
      jj_la1[37] = jj_gen;
      jj_consume_token(-1);
      throw new ParseException();
    }
        {if (true) return t.image;}
    throw new Error("Missing return statement in function");
  }

  private boolean jj_2_1(int xla) {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    try { return !jj_3_1(); }
    catch(LookaheadSuccess ls) { return true; }
    finally { jj_save(0, xla); }
  }

  private boolean jj_2_2(int xla) {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    try { return !jj_3_2(); }
    catch(LookaheadSuccess ls) { return true; }
    finally { jj_save(1, xla); }
  }

  private boolean jj_2_3(int xla) {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    try { return !jj_3_3(); }
    catch(LookaheadSuccess ls) { return true; }
    finally { jj_save(2, xla); }
  }

  private boolean jj_2_4(int xla) {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    try { return !jj_3_4(); }
    catch(LookaheadSuccess ls) { return true; }
    finally { jj_save(3, xla); }
  }

  private boolean jj_2_5(int xla) {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    try { return !jj_3_5(); }
    catch(LookaheadSuccess ls) { return true; }
    finally { jj_save(4, xla); }
  }

  private boolean jj_2_6(int xla) {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    try { return !jj_3_6(); }
    catch(LookaheadSuccess ls) { return true; }
    finally { jj_save(5, xla); }
  }

  private boolean jj_2_7(int xla) {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    try { return !jj_3_7(); }
    catch(LookaheadSuccess ls) { return true; }
    finally { jj_save(6, xla); }
  }

  private boolean jj_2_8(int xla) {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    try { return !jj_3_8(); }
    catch(LookaheadSuccess ls) { return true; }
    finally { jj_save(7, xla); }
  }

  private boolean jj_3_2() {
    if (jj_3R_8()) return true;
    if (jj_scan_token(44)) return true;
    if (jj_3R_8()) return true;
    return false;
  }

  private boolean jj_3_1() {
    if (jj_3R_8()) return true;
    if (jj_scan_token(44)) return true;
    if (jj_3R_8()) return true;
    if (jj_scan_token(44)) return true;
    if (jj_3R_8()) return true;
    return false;
  }

  private boolean jj_3R_42() {
    if (jj_scan_token(43)) return true;
    return false;
  }

  private boolean jj_3R_41() {
    Token xsp;
    xsp = jj_scanpos;
    if (jj_3R_42()) {
    jj_scanpos = xsp;
    if (jj_3_1()) {
    jj_scanpos = xsp;
    if (jj_3_2()) {
    jj_scanpos = xsp;
    if (jj_3R_43()) return true;
    }
    }
    }
    xsp = jj_scanpos;
    if (jj_3R_44()) jj_scanpos = xsp;
    return false;
  }

  private boolean jj_3R_30() {
    if (jj_3R_33()) return true;
    return false;
  }

  private boolean jj_3_7() {
    if (jj_scan_token(AND)) return true;
    if (jj_3R_12()) return true;
    return false;
  }

  private boolean jj_3R_25() {
    Token xsp;
    xsp = jj_scanpos;
    if (jj_3R_29()) {
    jj_scanpos = xsp;
    if (jj_3R_30()) return true;
    }
    return false;
  }

  private boolean jj_3R_29() {
    if (jj_3R_32()) return true;
    return false;
  }

  private boolean jj_3R_11() {
    if (jj_3R_19()) return true;
    Token xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_7()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

  private boolean jj_3R_40() {
    if (jj_3R_41()) return true;
    return false;
  }

  private boolean jj_3R_39() {
    if (jj_scan_token(SUM)) return true;
    return false;
  }

  private boolean jj_3R_38() {
    if (jj_scan_token(MIN)) return true;
    return false;
  }

  private boolean jj_3R_37() {
    if (jj_scan_token(MAX)) return true;
    return false;
  }

  private boolean jj_3R_36() {
    if (jj_scan_token(COUNT)) return true;
    return false;
  }

  private boolean jj_3_6() {
    if (jj_3R_11()) return true;
    return false;
  }

  private boolean jj_3R_33() {
    Token xsp;
    xsp = jj_scanpos;
    if (jj_scan_token(40)) {
    jj_scanpos = xsp;
    if (jj_scan_token(41)) {
    jj_scanpos = xsp;
    if (jj_scan_token(47)) return true;
    }
    }
    return false;
  }

  private boolean jj_3R_35() {
    if (jj_scan_token(AVG)) return true;
    return false;
  }

  private boolean jj_3R_32() {
    Token xsp;
    xsp = jj_scanpos;
    if (jj_3R_35()) {
    jj_scanpos = xsp;
    if (jj_3R_36()) {
    jj_scanpos = xsp;
    if (jj_3R_37()) {
    jj_scanpos = xsp;
    if (jj_3R_38()) {
    jj_scanpos = xsp;
    if (jj_3R_39()) {
    jj_scanpos = xsp;
    if (jj_3R_40()) return true;
    }
    }
    }
    }
    }
    return false;
  }

  private boolean jj_3R_34() {
    if (jj_scan_token(42)) return true;
    if (jj_3R_33()) return true;
    return false;
  }

  private boolean jj_3R_21() {
    if (jj_scan_token(45)) return true;
    if (jj_3R_10()) return true;
    if (jj_scan_token(46)) return true;
    return false;
  }

  private boolean jj_3R_20() {
    if (jj_3R_11()) return true;
    return false;
  }

  private boolean jj_3R_31() {
    if (jj_3R_33()) return true;
    Token xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3R_34()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

  private boolean jj_3R_26() {
    if (jj_scan_token(RELOP)) return true;
    return false;
  }

  private boolean jj_3R_12() {
    Token xsp;
    xsp = jj_scanpos;
    if (jj_3R_20()) {
    jj_scanpos = xsp;
    if (jj_3R_21()) return true;
    }
    return false;
  }

  private boolean jj_3R_28() {
    if (jj_3R_25()) return true;
    return false;
  }

  private boolean jj_3R_16() {
    if (jj_scan_token(MYSQL_DELIMITED_ID)) return true;
    return false;
  }

  private boolean jj_3R_27() {
    if (jj_scan_token(45)) return true;
    if (jj_3R_31()) return true;
    if (jj_scan_token(46)) return true;
    return false;
  }

  private boolean jj_3R_15() {
    if (jj_scan_token(DELIMITED_ID)) return true;
    return false;
  }

  private boolean jj_3_3() {
    if (jj_3R_8()) return true;
    if (jj_scan_token(44)) return true;
    return false;
  }

  private boolean jj_3R_24() {
    if (jj_3R_25()) return true;
    if (jj_3R_26()) return true;
    Token xsp;
    xsp = jj_scanpos;
    if (jj_3R_27()) {
    jj_scanpos = xsp;
    if (jj_3R_28()) return true;
    }
    return false;
  }

  private boolean jj_3R_14() {
    if (jj_scan_token(ORDINARY_ID)) return true;
    return false;
  }

  private boolean jj_3R_8() {
    Token xsp;
    xsp = jj_scanpos;
    if (jj_3R_14()) {
    jj_scanpos = xsp;
    if (jj_3R_15()) {
    jj_scanpos = xsp;
    if (jj_3R_16()) return true;
    }
    }
    return false;
  }

  private boolean jj_3_5() {
    if (jj_scan_token(OR)) return true;
    if (jj_3R_10()) return true;
    return false;
  }

  private boolean jj_3R_9() {
    if (jj_3R_12()) return true;
    Token xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_5()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

  private boolean jj_3_4() {
    if (jj_3R_9()) return true;
    return false;
  }

  private boolean jj_3R_23() {
    if (jj_scan_token(NOT)) return true;
    return false;
  }

  private boolean jj_3R_22() {
    Token xsp;
    xsp = jj_scanpos;
    if (jj_3R_23()) jj_scanpos = xsp;
    if (jj_3R_24()) return true;
    return false;
  }

  private boolean jj_3R_18() {
    if (jj_scan_token(45)) return true;
    if (jj_3R_10()) return true;
    if (jj_scan_token(46)) return true;
    return false;
  }

  private boolean jj_3R_17() {
    if (jj_3R_9()) return true;
    return false;
  }

  private boolean jj_3R_13() {
    if (jj_scan_token(NOT)) return true;
    return false;
  }

  private boolean jj_3_8() {
    Token xsp;
    xsp = jj_scanpos;
    if (jj_3R_13()) jj_scanpos = xsp;
    if (jj_scan_token(45)) return true;
    if (jj_3R_10()) return true;
    if (jj_scan_token(46)) return true;
    return false;
  }

  private boolean jj_3R_44() {
    Token xsp;
    xsp = jj_scanpos;
    if (jj_scan_token(5)) jj_scanpos = xsp;
    if (jj_3R_8()) return true;
    return false;
  }

  private boolean jj_3R_10() {
    Token xsp;
    xsp = jj_scanpos;
    if (jj_3R_17()) {
    jj_scanpos = xsp;
    if (jj_3R_18()) return true;
    }
    return false;
  }

  private boolean jj_3R_43() {
    if (jj_3R_8()) return true;
    return false;
  }

  private boolean jj_3R_19() {
    Token xsp;
    xsp = jj_scanpos;
    if (jj_3_8()) {
    jj_scanpos = xsp;
    if (jj_3R_22()) return true;
    }
    return false;
  }

  /** Generated Token Manager. */
  public SQLParserTokenManager token_source;
  JavaCharStream jj_input_stream;
  /** Current token. */
  public Token token;
  /** Next token. */
  public Token jj_nt;
  private int jj_ntk;
  private Token jj_scanpos, jj_lastpos;
  private int jj_la;
  private int jj_gen;
  final private int[] jj_la1 = new int[38];
  static private int[] jj_la1_0;
  static private int[] jj_la1_1;
  static {
      jj_la1_init_0();
      jj_la1_init_1();
   }
   private static void jj_la1_init_0() {
      jj_la1_0 = new int[] {0x0,0x20000,0x40000,0x10000000,0x4040,0x4040,0x0,0xc00a00,0x0,0x0,0x20,0x20,0x20,0x20,0x61391000,0x0,0x4000000,0x0,0x20,0x20,0x40110000,0x40110000,0x61191000,0x61191000,0x61391000,0x0,0x0,0x2000000,0x2000000,0x2c00a00,0xc00a00,0x0,0xc00a00,0x0,0x2100,0x2100,0x0,0x0,};
   }
   private static void jj_la1_init_1() {
      jj_la1_1 = new int[] {0x2,0x0,0x0,0x0,0x0,0x0,0x400,0x839,0x800,0x38,0x0,0x38,0x0,0x38,0x400,0x400,0x0,0x38,0x0,0x38,0x0,0x0,0x0,0x0,0x400,0x2000,0x2000,0x0,0x0,0x8b39,0xab39,0x400,0x8b39,0x400,0x0,0x0,0x38,0x8300,};
   }
  final private JJCalls[] jj_2_rtns = new JJCalls[8];
  private boolean jj_rescan = false;
  private int jj_gc = 0;

  /** Constructor with InputStream. */
  public SQLParser(java.io.InputStream stream) {
     this(stream, null);
  }
  /** Constructor with InputStream and supplied encoding */
  public SQLParser(java.io.InputStream stream, String encoding) {
    try {
      jj_input_stream = new JavaCharStream(stream, encoding, 1, 1);
    } catch(java.io.UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
    token_source = new SQLParserTokenManager(jj_input_stream);
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 38; i++) jj_la1[i] = -1;
    for (int i = 0; i < jj_2_rtns.length; i++) jj_2_rtns[i] = new JJCalls();
  }

  /** Reinitialise. */
  public void ReInit(java.io.InputStream stream) {
     ReInit(stream, null);
  }
  /** Reinitialise. */
  public void ReInit(java.io.InputStream stream, String encoding) {
    try { jj_input_stream.ReInit(stream, encoding, 1, 1); } catch(java.io.UnsupportedEncodingException e) { throw new RuntimeException(e); }
    token_source.ReInit(jj_input_stream);
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 38; i++) jj_la1[i] = -1;
    for (int i = 0; i < jj_2_rtns.length; i++) jj_2_rtns[i] = new JJCalls();
  }

  /** Constructor. */
  public SQLParser(java.io.Reader stream) {
    jj_input_stream = new JavaCharStream(stream, 1, 1);
    token_source = new SQLParserTokenManager(jj_input_stream);
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 38; i++) jj_la1[i] = -1;
    for (int i = 0; i < jj_2_rtns.length; i++) jj_2_rtns[i] = new JJCalls();
  }

  /** Reinitialise. */
  public void ReInit(java.io.Reader stream) {
    jj_input_stream.ReInit(stream, 1, 1);
    token_source.ReInit(jj_input_stream);
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 38; i++) jj_la1[i] = -1;
    for (int i = 0; i < jj_2_rtns.length; i++) jj_2_rtns[i] = new JJCalls();
  }

  /** Constructor with generated Token Manager. */
  public SQLParser(SQLParserTokenManager tm) {
    token_source = tm;
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 38; i++) jj_la1[i] = -1;
    for (int i = 0; i < jj_2_rtns.length; i++) jj_2_rtns[i] = new JJCalls();
  }

  /** Reinitialise. */
  public void ReInit(SQLParserTokenManager tm) {
    token_source = tm;
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 38; i++) jj_la1[i] = -1;
    for (int i = 0; i < jj_2_rtns.length; i++) jj_2_rtns[i] = new JJCalls();
  }

  private Token jj_consume_token(int kind) throws ParseException {
    Token oldToken;
    if ((oldToken = token).next != null) token = token.next;
    else token = token.next = token_source.getNextToken();
    jj_ntk = -1;
    if (token.kind == kind) {
      jj_gen++;
      if (++jj_gc > 100) {
        jj_gc = 0;
        for (int i = 0; i < jj_2_rtns.length; i++) {
          JJCalls c = jj_2_rtns[i];
          while (c != null) {
            if (c.gen < jj_gen) c.first = null;
            c = c.next;
          }
        }
      }
      return token;
    }
    token = oldToken;
    jj_kind = kind;
    throw generateParseException();
  }

  static private final class LookaheadSuccess extends java.lang.Error { }
  final private LookaheadSuccess jj_ls = new LookaheadSuccess();
  private boolean jj_scan_token(int kind) {
    if (jj_scanpos == jj_lastpos) {
      jj_la--;
      if (jj_scanpos.next == null) {
        jj_lastpos = jj_scanpos = jj_scanpos.next = token_source.getNextToken();
      } else {
        jj_lastpos = jj_scanpos = jj_scanpos.next;
      }
    } else {
      jj_scanpos = jj_scanpos.next;
    }
    if (jj_rescan) {
      int i = 0; Token tok = token;
      while (tok != null && tok != jj_scanpos) { i++; tok = tok.next; }
      if (tok != null) jj_add_error_token(kind, i);
    }
    if (jj_scanpos.kind != kind) return true;
    if (jj_la == 0 && jj_scanpos == jj_lastpos) throw jj_ls;
    return false;
  }


/** Get the next Token. */
  final public Token getNextToken() {
    if (token.next != null) token = token.next;
    else token = token.next = token_source.getNextToken();
    jj_ntk = -1;
    jj_gen++;
    return token;
  }

/** Get the specific Token. */
  final public Token getToken(int index) {
    Token t = token;
    for (int i = 0; i < index; i++) {
      if (t.next != null) t = t.next;
      else t = t.next = token_source.getNextToken();
    }
    return t;
  }

  private int jj_ntk() {
    if ((jj_nt=token.next) == null)
      return (jj_ntk = (token.next=token_source.getNextToken()).kind);
    else
      return (jj_ntk = jj_nt.kind);
  }

  private java.util.List<int[]> jj_expentries = new java.util.ArrayList<int[]>();
  private int[] jj_expentry;
  private int jj_kind = -1;
  private int[] jj_lasttokens = new int[100];
  private int jj_endpos;

  private void jj_add_error_token(int kind, int pos) {
    if (pos >= 100) return;
    if (pos == jj_endpos + 1) {
      jj_lasttokens[jj_endpos++] = kind;
    } else if (jj_endpos != 0) {
      jj_expentry = new int[jj_endpos];
      for (int i = 0; i < jj_endpos; i++) {
        jj_expentry[i] = jj_lasttokens[i];
      }
      jj_entries_loop: for (java.util.Iterator<?> it = jj_expentries.iterator(); it.hasNext();) {
        int[] oldentry = (int[])(it.next());
        if (oldentry.length == jj_expentry.length) {
          for (int i = 0; i < jj_expentry.length; i++) {
            if (oldentry[i] != jj_expentry[i]) {
              continue jj_entries_loop;
            }
          }
          jj_expentries.add(jj_expentry);
          break jj_entries_loop;
        }
      }
      if (pos != 0) jj_lasttokens[(jj_endpos = pos) - 1] = kind;
    }
  }

  /** Generate ParseException. */
  public ParseException generateParseException() {
    jj_expentries.clear();
    boolean[] la1tokens = new boolean[48];
    if (jj_kind >= 0) {
      la1tokens[jj_kind] = true;
      jj_kind = -1;
    }
    for (int i = 0; i < 38; i++) {
      if (jj_la1[i] == jj_gen) {
        for (int j = 0; j < 32; j++) {
          if ((jj_la1_0[i] & (1<<j)) != 0) {
            la1tokens[j] = true;
          }
          if ((jj_la1_1[i] & (1<<j)) != 0) {
            la1tokens[32+j] = true;
          }
        }
      }
    }
    for (int i = 0; i < 48; i++) {
      if (la1tokens[i]) {
        jj_expentry = new int[1];
        jj_expentry[0] = i;
        jj_expentries.add(jj_expentry);
      }
    }
    jj_endpos = 0;
    jj_rescan_token();
    jj_add_error_token(0, 0);
    int[][] exptokseq = new int[jj_expentries.size()][];
    for (int i = 0; i < jj_expentries.size(); i++) {
      exptokseq[i] = jj_expentries.get(i);
    }
    return new ParseException(token, exptokseq, tokenImage);
  }

  /** Enable tracing. */
  final public void enable_tracing() {
  }

  /** Disable tracing. */
  final public void disable_tracing() {
  }

  private void jj_rescan_token() {
    jj_rescan = true;
    for (int i = 0; i < 8; i++) {
    try {
      JJCalls p = jj_2_rtns[i];
      do {
        if (p.gen > jj_gen) {
          jj_la = p.arg; jj_lastpos = jj_scanpos = p.first;
          switch (i) {
            case 0: jj_3_1(); break;
            case 1: jj_3_2(); break;
            case 2: jj_3_3(); break;
            case 3: jj_3_4(); break;
            case 4: jj_3_5(); break;
            case 5: jj_3_6(); break;
            case 6: jj_3_7(); break;
            case 7: jj_3_8(); break;
          }
        }
        p = p.next;
      } while (p != null);
      } catch(LookaheadSuccess ls) { }
    }
    jj_rescan = false;
  }

  private void jj_save(int index, int xla) {
    JJCalls p = jj_2_rtns[index];
    while (p.gen > jj_gen) {
      if (p.next == null) { p = p.next = new JJCalls(); break; }
      p = p.next;
    }
    p.gen = jj_gen + xla - jj_la; p.first = token; p.arg = xla;
  }

  static final class JJCalls {
    int gen;
    Token first;
    int arg;
    JJCalls next;
  }

}
