export class PromiseArgs {
    receiver: string;
    methodName: string;
    args: PromiseArgs;
    balance: i32;
    callback: string;
    callbackArgs: PromiseArgs;
    callbackBalance: i32;
}

export class InputPromiseArgs {
    args: PromiseArgs;
}

export class MyContractPromiseResult {
    ok: bool;
    r: MyCallbackResult;
  }

export class MyCallbackResult {
    rs: MyContractPromiseResult[];
    n: string;
}
