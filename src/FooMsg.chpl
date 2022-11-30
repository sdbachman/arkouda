module FooMsg
{

  use GenSymIO;
  use SymEntry4D;
  use Arr4DMsg;

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


    // do foo on arrays a and b
    proc foo(a: [?aD] int, b: [?bD] int): [aD] int {
       var ret = a + b;
       return(ret);
    }

    /*
    Parse, execute, and respond to a foo message
    :arg reqMsg: request containing (cmd,payload,argSize)
    :type reqMsg: string
    :arg st: SymTab to act on
    :type st: borrowed SymTab
    :returns: (MsgTuple) response message
    */


    proc fooMsg(cmd: string, payload: string, argSize: int, st: borrowed SymTab): MsgTuple throws {

        param pn = Reflection.getRoutineName();
        var repMsg: string; // response message

        // split request into fields
        var msgArgs = parseMessageArgs(payload, argSize);

        const op = "+";
        var aname = msgArgs.getValueOf("aname");
        var bname = msgArgs.getValueOf("bname");
        
        // get next symbol name
        var rname = st.nextName();
        var left: borrowed GenSymEntry = getGenericTypedArrayEntry(aname, st);
        var right: borrowed GenSymEntry = getGenericTypedArrayEntry(bname, st);



        //  when (DType.Int64, DType.Int64) {
            var l = left: SymEntry4D(int);
            var r = right: SymEntry4D(int);

            var e = st.addEntry4D(rname, l.m, l.n, l.p, l.q, int);
            return doBinOpvv(l, r, e, op, rname, pn, st);
        //  }


    }


use CommandMap;
registerFunction("foo", fooMsg, getModuleName());

}


