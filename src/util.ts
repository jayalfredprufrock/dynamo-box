// eslint-disable-next-line @typescript-eslint/no-explicit-any
export const removeUndefined = (obj: any) => {
    const stack = [obj];
    while (stack.length) {
        const currentObj = stack.pop();
        if (currentObj !== undefined) {
            Object.entries(currentObj).forEach(([k, v]) => {
                if (v && v instanceof Object) stack.push(v);
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                else if (v === undefined) delete (currentObj as any)[k];
            });
        }
    }
    return obj;
};
