module Arr4DMsg {
  use GenSymIO;
  use Arr2DMsg;
  use Arr3DMsg;
  use SymEntry2D;
  use SymEntry3D;
  use SymEntry4D;

  use List;
  use ServerConfig;
  use MultiTypeSymbolTable;
  use MultiTypeSymEntry;
  use Message;
  use ServerErrors;
  use Reflection;
  use RandArray;
  use Logging;
  use ServerErrorStrings;

  use BinOp;

  private config const logLevel = ServerConfig.logLevel;
  const randLogger = new Logger(logLevel);

  proc array4DMsg(cmd: string, payload: string, argSize: int, st: borrowed SymTab): MsgTuple throws {
    var msgArgs = parseMessageArgs(payload, argSize);

    var val = msgArgs.getValueOf("val");
    var dtype = DType.UNDEF;
    var m: int;
    var n: int;
    var p: int;
    var q: int;
    var rname:string = "";

    try {
      dtype = str2dtype(msgArgs.getValueOf("dtype"));
      m = msgArgs.get("m").getIntValue();
      n = msgArgs.get("n").getIntValue();
      p = msgArgs.get("p").getIntValue();
      q = msgArgs.get("q").getIntValue();
    } catch {
      var errorMsg = "Error parsing/decoding either dtypeBytes, m, n, p, or q";
      gsLogger.error(getModuleName(), getRoutineName(), getLineNumber(), errorMsg);
      return new MsgTuple(errorMsg, MsgType.ERROR);
    }

    overMemLimit(2*m*n*p*q);

    if dtype == DType.Int64 {
      var entry = new shared SymEntry4D(m, n, p, q, int);
      var localA: [{0..#m, 0..#n, 0..#p, 0..#q}] int = val:int;
      entry.a = localA;
      rname = st.nextName();
      st.addEntry(rname, entry);
    } else if dtype == DType.Float64 {
      var entry = new shared SymEntry4D(m, n, p, q, real);
      var localA: [{0..#m, 0..#n, 0..#p, 0..#q}] real = val:real;
      entry.a = localA;
      rname = st.nextName();
      st.addEntry(rname, entry);
    } else if dtype == DType.Bool {
      var entry = new shared SymEntry4D(m, n, p, q, bool);
      var localA: [{0..#m, 0..#n, 0..#p, 0..#q}] bool = if val == "True" then true else false;
      entry.a = localA;
      rname = st.nextName();
      st.addEntry(rname, entry);
    }

    var msgType = MsgType.NORMAL;
    var msg:string = "";

    if (MsgType.ERROR != msgType) {
      if (msg.isEmpty()) {
        msg = "created " + st.attrib(rname);
      }
      gsLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),msg);
    }
    return new MsgTuple(msg, msgType);
  }

  proc randint4DMsg(cmd: string, payload: string, argSize: int, st: borrowed SymTab): MsgTuple throws {
    param pn = Reflection.getRoutineName();
    var repMsg: string; // response message
    var msgArgs = parseMessageArgs(payload, argSize);

    var dtype = str2dtype(msgArgs.getValueOf("dtype"));
    var m = msgArgs.get("m").getIntValue();
    var n = msgArgs.get("n").getIntValue();
    var p = msgArgs.get("p").getIntValue();
    var q = msgArgs.get("q").getIntValue();
    var rname = st.nextName();

    select (dtype) {
      when (DType.Int64) {
        overMemLimit(8*m*n*p*q);
        var aMin = msgArgs.get("low").getIntValue();
        var aMax = msgArgs.get("high").getIntValue();
        var seed = msgArgs.getValueOf("seed");

        var entry = new shared SymEntry4D(m, n, p, q, int);
        var localA: [{0..#m, 0..#n, 0..#p, 0..#q}] int;
        entry.a = localA;
        st.addEntry(rname, entry);
        fillInt(entry.a, aMin, aMax, seed);
      }
      when (DType.Float64) {
        var seed = msgArgs.getValueOf("seed");
        overMemLimit(8*m*n*p*q);
        var aMin = msgArgs.get("low").getRealValue();
        var aMax = msgArgs.get("high").getRealValue();

        var entry = new shared SymEntry4D(m, n, p, q, real);
        var localA: [{0..#m, 0..#n, 0..#p, 0..#q}] real;
        entry.a = localA;
        st.addEntry(rname, entry);
        fillReal(entry.a, aMin, aMax, seed);
      }
      when (DType.Bool) {
        var seed = msgArgs.getValueOf("seed");
        overMemLimit(8*m*n*p*q);

        var entry = new shared SymEntry4D(m, n, p, q, bool);
        var localA: [{0..#m, 0..#n, 0..#p, 0..#q}] bool;
        entry.a = localA;
        st.addEntry(rname, entry);
        fillBool(entry.a, seed);
      }
      otherwise {
        var errorMsg = notImplementedError(pn,dtype);
        randLogger.error(getModuleName(),getRoutineName(),getLineNumber(),errorMsg);
        return new MsgTuple(errorMsg, MsgType.ERROR);
      }
    }

    repMsg = "created " + st.attrib(rname);
    return new MsgTuple(repMsg, MsgType.NORMAL);
  }

  proc binopvv4DMsg(cmd: string, payload: string, argSize: int, st: borrowed SymTab): MsgTuple throws {
    param pn = Reflection.getRoutineName();
    var repMsg: string; // response message

    var msgArgs = parseMessageArgs(payload, argSize);

    const op = msgArgs.getValueOf("op");
    var aname = msgArgs.getValueOf("a");
    var bname = msgArgs.getValueOf("b");

    var rname = st.nextName();
    var left: borrowed GenSymEntry = getGenericTypedArrayEntry(aname, st);
    var right: borrowed GenSymEntry = getGenericTypedArrayEntry(bname, st);

    use Set;
    var boolOps: set(string);
    boolOps.add("<");
    boolOps.add("<=");
    boolOps.add(">");
    boolOps.add(">=");
    boolOps.add("==");
    boolOps.add("!=");

    select (left.dtype, right.dtype) {
      when (DType.Int64, DType.Int64) {
        var l = left: SymEntry4D(int);
        var r = right: SymEntry4D(int);
        if boolOps.contains(op) {
          var e = st.addEntry4D(rname, l.m, l.n, l.p, l.q, bool);
          return doBinOpvv(l, r, e, op, rname, pn, st);
        } else if op == "/" {
          var e = st.addEntry4D(rname, l.m, l.n, l.p, l.q, real);
          return doBinOpvv(l, r, e, op, rname, pn, st);
        }
        var e = st.addEntry4D(rname, l.m, l.n, l.p, l.q, int);
        return doBinOpvv(l, r, e, op, rname, pn, st);
      }
      when (DType.Int64, DType.Float64) {
        var l = left: SymEntry4D(int);
        var r = right: SymEntry4D(real);
        if boolOps.contains(op) {
          var e = st.addEntry4D(rname, l.m, l.n, l.p, l.q, bool);
          return doBinOpvv(l, r, e, op, rname, pn, st);
        }
        var e = st.addEntry4D(rname, l.m, l.n, l.p, l.q, real);
        return doBinOpvv(l, r, e, op, rname, pn, st);
      }
      when (DType.Float64, DType.Int64) {
        var l = left: SymEntry4D(real);
        var r = right: SymEntry4D(int);
        if boolOps.contains(op) {
          var e = st.addEntry4D(rname, l.m, l.n, l.p, l.q, bool);
          return doBinOpvv(l, r, e, op, rname, pn, st);
        }
        var e = st.addEntry4D(rname, l.m, l.n, l.p, l.q, real);
        return doBinOpvv(l, r, e, op, rname, pn, st);
      }
      when (DType.Float64, DType.Float64) {
        var l = left: SymEntry4D(real);
        var r = right: SymEntry4D(real);
        if boolOps.contains(op) {
          var e = st.addEntry4D(rname, l.m, l.n, l.p, l.q, bool);
          return doBinOpvv(l, r, e, op, rname, pn, st);
        }
        var e = st.addEntry4D(rname, l.m, l.n, l.p, l.q, real);
        return doBinOpvv(l, r, e, op, rname, pn, st);
      }
      when (DType.Bool, DType.Bool) {
        var l = left: SymEntry4D(bool);
        var r = right: SymEntry4D(bool);
        var e = st.addEntry4D(rname, l.m, l.n, l.p, l.q, bool);
        return doBinOpvv(l, r, e, op, rname, pn, st);
      }
      when (DType.Bool, DType.Int64) {
        var l = left: SymEntry4D(bool);
        var r = right: SymEntry4D(int);
        var e = st.addEntry4D(rname, l.m, l.n, l.p, l.q, int);
        return doBinOpvv(l, r, e, op, rname, pn, st);
      }
      when (DType.Int64, DType.Bool) {
        var l = left: SymEntry4D(int);
        var r = right: SymEntry4D(bool);
        var e = st.addEntry4D(rname, l.m, l.n, l.p, l.q, int);
        return doBinOpvv(l, r, e, op, rname, pn, st);
      }
      when (DType.Bool, DType.Float64) {
        var l = left: SymEntry4D(bool);
        var r = right: SymEntry4D(real);
        var e = st.addEntry4D(rname, l.m, l.n, l.p, l.q, real);
        return doBinOpvv(l, r, e, op, rname, pn, st);
      }
      when (DType.Float64, DType.Bool) {
        var l = left: SymEntry4D(real);
        var r = right: SymEntry4D(bool);
        var e = st.addEntry4D(rname, l.m, l.n, l.p, l.q, real);
        return doBinOpvv(l, r, e, op, rname, pn, st);
      }
    }
    return new MsgTuple("Bin op not supported", MsgType.NORMAL);
  }

  proc SymTab.addEntry4D(name: string, m, n, p, q, type t): borrowed SymEntry4D(t) throws {
    if t == bool {overMemLimit(m*n*p*q);} else {overMemLimit(m*n*p*q*numBytes(t));}

    var entry = new shared SymEntry4D(m, n, p, q, t);
    if (tab.contains(name)) {
      mtLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                     "redefined symbol: %s ".format(name));
    } else {
      mtLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                     "adding symbol: %s ".format(name));
    }

    tab.addOrSet(name, entry);
    return (tab.getBorrowed(name):borrowed GenSymEntry): SymEntry4D(t);
  }

  proc partialReduction4DMsg(cmd: string, payload: string, argSize: int, st: borrowed SymTab): MsgTuple throws {
    var msgArgs = parseMessageArgs(payload, argSize);
    var repMsg: string;

    var name = msgArgs.getValueOf("name");
    /*
    var axis: int;
    try {
      axis = msgArgs.get("axis").getIntValue();
    } catch {
      var errorMsg = "Error parsing/decoding key";
      gsLogger.error(getModuleName(), getRoutineName(), getLineNumber(), errorMsg);
    }
    */
    var axis = msgArgs.getValueOf("axis");
    writeln("axis: ", axis);
    writeln("type: ", axis.type : string);

    var inputEntry: borrowed GenSymEntry = getGenericTypedArrayEntry(name, st);
    // TODO: why is this type real when I pass 5?

    // TODO: select on type
    var inputArr = inputEntry: SymEntry4D(real);
    var rnames: list((string, string, string));
    var rname = st.nextName();

    // If no "axis" input argument
    if (axis == "None") { // sum whole array
      var res = st.addEntry(rname, 1, real);
      res.a = + reduce inputArr.a;
      rnames.append((name, "pdarray", rname));
    }
    else { // If "axis" is not None
      var axis_int : int = -1;
      try { // If "axis" can be cast to an integer
        var axis_int = axis : int;

        // TODO: select on op and type
        if axis_int == 0 {
          var num_axis1 = inputArr.n;
          var num_axis2 = inputArr.p;
          var num_axis3 = inputArr.q;
          var res = st.addEntry3D(rname, num_axis1, num_axis2, num_axis3, real);
          forall i in 0..#num_axis1 {
            for j in 0..#num_axis2 {
              for k in 0..#num_axis3 {
                res.a[i,j,k] = + reduce inputArr.a[..,i,j,k];
              }
            }
          }
          rnames.append((name, "pdarray3D", rname));
        } else if axis_int == 1 {
          var num_axis0 = inputArr.m;
          var num_axis2 = inputArr.p;
          var num_axis3 = inputArr.q;
          var res = st.addEntry3D(rname, num_axis0, num_axis2, num_axis3, real);
          forall i in 0..#num_axis0 {
            for j in 0..#num_axis2 {
              for k in 0..#num_axis3 {
                res.a[i,j,k] = + reduce inputArr.a[i,..,j,k];
              }
            }
          }
          rnames.append((name, "pdarray3D", rname));
        }  else if axis_int == 2 {
          var num_axis0 = inputArr.m;
          var num_axis1 = inputArr.n;
          var num_axis3 = inputArr.q;
          var res = st.addEntry3D(rname, num_axis0, num_axis1, num_axis3, real);
          forall i in 0..#num_axis0 {
            for j in 0..#num_axis1 {
              for k in 0..#num_axis3 {
                res.a[i,j,k] = + reduce inputArr.a[i,j,..,k];
              }
            }
          }
          rnames.append((name, "pdarray3D", rname));
        }  else if axis_int == 3 {
          var num_axis0 = inputArr.m;
          var num_axis1 = inputArr.n;
          var num_axis2 = inputArr.p;
          var res = st.addEntry3D(rname, num_axis0, num_axis1, num_axis2, real);
          forall i in 0..#num_axis0 {
            for j in 0..#num_axis1 {
              for k in 0..#num_axis2 {
                res.a[i,j,k] = + reduce inputArr.a[i,j,k,..];
              }
            }
          }
          rnames.append((name, "pdarray3D", rname));
        } else {
          var errorMsg = "axis " + axis_int : string + " is out of bounds for array of dimension 3";
          gsLogger.error(getModuleName(), getRoutineName(), getLineNumber(), errorMsg);
        }
      }
      catch { // See if "axis" is a tuple
        if (axis[0] == "(" && axis[axis.size-1] == ")") {
          var axis_elements = axis.strip("()").split(",");

          var axis_int = check_elements(axis_elements);
          if (axis_int == 1) {
            var num_axis1 = inputArr.n;
            var num_axis2 = inputArr.p;
            var num_axis3 = inputArr.q;
            var res = st.addEntry3D(rname, num_axis1, num_axis2, num_axis3, real);
            forall i in 0..#num_axis1 {
              for j in 0..#num_axis2 {
                for k in 0..#num_axis3 {
                  res.a[i,j,k] = + reduce inputArr.a[..,i,j,k];
                }
              }
            }
            rnames.append((name, "pdarray3D", rname));
          }
          else if (axis_int == 2) {
            var num_axis0 = inputArr.m;
            var num_axis2 = inputArr.p;
            var num_axis3 = inputArr.q;
            var res = st.addEntry3D(rname, num_axis0, num_axis2, num_axis3, real);
            forall i in 0..#num_axis0 {
              for j in 0..#num_axis2 {
                for k in 0..#num_axis3 {
                  res.a[i,j,k] = + reduce inputArr.a[i,..,j,k];
                }
              }
            }
            rnames.append((name, "pdarray3D", rname));
          }
          else if (axis_int == 3) {
            var num_axis2 = inputArr.p;
            var num_axis3 = inputArr.q;
            var res = st.addEntry2D(rname, num_axis2, num_axis3, real);
            forall i in 0..#num_axis2 {
              for j in 0..#num_axis3 {
                res.a[i,j] = + reduce inputArr.a[..,..,i,j];
              }
            }
            rnames.append((name, "pdarray2D", rname));
          }
          else if (axis_int == 4) {
            var num_axis0 = inputArr.m;
            var num_axis1 = inputArr.n;
            var num_axis3 = inputArr.q;
            var res = st.addEntry3D(rname, num_axis0, num_axis1, num_axis3, real);
            forall i in 0..#num_axis0 {
              for j in 0..#num_axis1 {
                for k in 0..#num_axis3 {
                  res.a[i,j,k] = + reduce inputArr.a[i,j,..,k];
                }
              }
            }
            rnames.append((name, "pdarray3D", rname));
          }
          else if (axis_int == 5) {
            var num_axis1 = inputArr.n;
            var num_axis3 = inputArr.q;
            var res = st.addEntry2D(rname, num_axis1, num_axis3, real);
            forall i in 0..#num_axis1 {
              for j in 0..#num_axis3 {
                res.a[i,j] = + reduce inputArr.a[..,i,..,j];
              }
            }
            rnames.append((name, "pdarray2D", rname));
          }
          else if (axis_int == 6) {
            var num_axis0 = inputArr.m;
            var num_axis3 = inputArr.q;
            var res = st.addEntry2D(rname, num_axis0, num_axis3, real);
            forall i in 0..#num_axis0 {
              for j in 0..#num_axis3 {
                res.a[i,j] = + reduce inputArr.a[i,..,..,j];
              }
            }
            rnames.append((name, "pdarray2D", rname));
          }
          else if (axis_int == 7) {
            var num_axis3 = inputArr.q;
            var res = st.addEntry(rname, num_axis3, real);
            forall i in 0..#num_axis3 {
              res.a[i] = + reduce inputArr.a[..,..,..,i];
            }
            rnames.append((name, "pdarray", rname));
          }
          else if (axis_int == 8) {
            var num_axis0 = inputArr.m;
            var num_axis1 = inputArr.n;
            var num_axis2 = inputArr.p;
            var res = st.addEntry3D(rname, num_axis0, num_axis1, num_axis2, real);
            forall i in 0..#num_axis0 {
              for j in 0..#num_axis1 {
                for k in 0..#num_axis2 {
                  res.a[i,j,k] = + reduce inputArr.a[i,j,k,..];
                }
              }
            }
            rnames.append((name, "pdarray3D", rname));
          }
          else if (axis_int == 9) {
            var num_axis1 = inputArr.n;
            var num_axis2 = inputArr.p;
            var res = st.addEntry2D(rname, num_axis1, num_axis2, real);
            forall i in 0..#num_axis1 {
              for j in 0..#num_axis2 {
                res.a[i,j] = + reduce inputArr.a[..,i,j,..];
              }
            }
            rnames.append((name, "pdarray2D", rname));
          }
          else if (axis_int == 10) {
            var num_axis0 = inputArr.m;
            var num_axis2 = inputArr.p;
            var res = st.addEntry2D(rname, num_axis0, num_axis2, real);
            forall i in 0..#num_axis0 {
              for j in 0..#num_axis2 {
                res.a[i,j] = + reduce inputArr.a[i,..,j,..];
              }
            }
            rnames.append((name, "pdarray2D", rname));
          }
          else if (axis_int == 11) {
            var num_axis2 = inputArr.p;
            var res = st.addEntry(rname, num_axis2, real);
            forall i in 0..#num_axis2 {
              res.a[i] = + reduce inputArr.a[..,..,i,..];
            }
            rnames.append((name, "pdarray", rname));
          }
          else if (axis_int == 12) {
            var num_axis0 = inputArr.m;
            var num_axis1 = inputArr.n;
            var res = st.addEntry2D(rname, num_axis0, num_axis1, real);
            forall i in 0..#num_axis0 {
              for j in 0..#num_axis1 {
                res.a[i,j] = + reduce inputArr.a[i,j,..,..];
              }
            }
            rnames.append((name, "pdarray2D", rname));
          }
          else if (axis_int == 13) {
            var num_axis1 = inputArr.n;
            var res = st.addEntry(rname, num_axis1, real);
            forall i in 0..#num_axis1 {
              res.a[i] = + reduce inputArr.a[..,i,..,..];
            }
            rnames.append((name, "pdarray", rname));
          }
          else if (axis_int == 14) {
            var num_axis0 = inputArr.m;
            var res = st.addEntry(rname, num_axis0, real);
            forall i in 0..#num_axis0 {
              res.a[i] = + reduce inputArr.a[i,..,..,..];
            }
            rnames.append((name, "pdarray", rname));
          }
          else if (axis_int == 15) { // sum whole array
            var res = st.addEntry(rname, 1, real);
            res.a = + reduce inputArr.a;
            rnames.append((name, "pdarray", rname));
          }
          else {
            var errorMsg = "Problem with input tuple.";
            gsLogger.error(getModuleName(), getRoutineName(), getLineNumber(), errorMsg);
          }
        }
        else { // If "axis" is not an int or a tuple
          var errorMsg = "Argument 'axis' is not an int or a tuple.";
          gsLogger.error(getModuleName(), getRoutineName(), getLineNumber(), errorMsg);
          // return error message?
        }
      }

    }

    var allowErrors = false;
    var fileErrorCount:int = 0;
    var fileErrors: list(string);
    var fileErrorMsg:string = "";
    repMsg = _buildReadAllMsgJson(rnames, allowErrors, fileErrorCount, fileErrors, st);

    //repMsg = "created " + st.attrib(rname);
    return new MsgTuple(repMsg, MsgType.NORMAL);
  }

  proc check_elements(axis_elements) throws {
    // Can all elements be cast to integers?
    for element in axis_elements {
      try {
        var el_int = element : int;
      }
      catch {
        writeln("Element included in tuple cannot be cast to an integer.");
        return 0;
      }
    }

    // Are all elements 0, 1, 2, or 3?
    for element in axis_elements {
      var el_int = element : int;
      if (el_int < 0 || el_int > 3) {
        writeln("axis ", el_int, " is out of bounds for array of dimension 4");
        return 0;
      }
    }

    var (zero_flag, one_flag, two_flag, three_flag) = (0,0,0,0);
    var el_sum = 0;
    for element in axis_elements {
      var el_int = element : int;
      if (el_int == 0) {
        if (zero_flag == 0) {
          el_sum += 1;
          zero_flag = 1;
        }
        else {
          writeln("duplicate value of 0 in 'axis'");
          return 0;
        }
      }
      if (el_int == 1) {
        if (one_flag == 0) {
          el_sum += 2;
          one_flag = 1;
        }
        else {
          writeln("duplicate value of 1 in 'axis'");
          return 0;
        }
      }

      if (el_int == 2) {
        if (two_flag == 0) {
          el_sum += 4;
          two_flag = 1;
        }
        else {
          writeln("duplicate value of 2 in 'axis'");
          return 0;
        }
      }

      if (el_int == 3) {
        if (three_flag == 0) {
          el_sum += 8;
          three_flag = 1;
        }
        else {
          writeln("duplicate value of 3 in 'axis'");
          return 0;
        }
      }
    }

    return el_sum;
  }

  use CommandMap;
  registerFunction("array4d", array4DMsg,getModuleName());
  registerFunction("randint4d", randint4DMsg,getModuleName());
  registerFunction("binopvv4d", binopvv4DMsg,getModuleName());
  registerFunction("partialReduction4D", partialReduction4DMsg, getModuleName());
}
