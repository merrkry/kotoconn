import type {
  Reference,
  OtherReference,
  Options,
  Reply,
  Mode,
  Choice,
  Collections,
  NativeUnion,
  Handler,
  Service,
} from "./generated.js";

type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

type Assert<T extends true> = T;

export type Cases = [
  Assert<Equal<Options["parent"], Reference | null>>,
  Assert<Equal<Options["values"], number[]>>,
  Assert<Equal<Options["details"], { label: string; enabled: boolean }>>,
  Assert<Equal<Mode, "enabled" | "disabled">>,
  Assert<Equal<Extract<Choice, { kind: "pair" }>["value"], [string, number]>>,
  Assert<Equal<Collections["tags"], string[]>>,
  Assert<Equal<Collections["table"], Record<string, number[]>>>,
  Assert<Equal<Extract<NativeUnion, { version: 6 }>["version"], 6>>,
  Assert<Equal<Parameters<Service["count"]>[0], Options>>,
  Assert<Equal<ReturnType<Service["echo"]>, Reply>>,
  Assert<Equal<ReturnType<Service["echo_later"]>, Promise<Reply>>>,
  Assert<Equal<Parameters<Service["register"]>[0], Handler<Reply, Reply>>>,
];

// The values need no native runtime. tsc validates both allowed and rejected shapes.
export function assignments(reference: Reference, other: OtherReference, service: Service) {
  const options: Options = {
    details: { label: "value", enabled: true },
    parent: null,
    values: [1, 2],
  };
  options.parent = reference;
  service.count(options);
  service.register((input) => input);
  service.register(async (input) => input);

  // @ts-expect-error Distinct native classes are not interchangeable.
  options.parent = other;

  // @ts-expect-error Required fields cannot disappear from option objects.
  service.count({ parent: null, values: [] });

  // @ts-expect-error Array element types come from Rust.
  options.values = ["1"];

  // @ts-expect-error Callbacks must return their declared result type.
  service.register(() => "wrong result");

  // @ts-expect-error Promise results must match the callback output too.
  service.register(async () => "wrong result");

  // @ts-expect-error Async methods return a promise, not the resolved object.
  const reply: Reply = service.echo_later({ label: "pending", count: 0 });

  // @ts-expect-error A native reference is not a JavaScript numeric ID.
  options.parent = 1;

  const choice: Choice = { kind: "named", value: { name: "item" } };

  // @ts-expect-error Tagged variants must carry the matching payload.
  const wrong: Choice = { kind: "pair", value: { name: "item" } };

  return [choice, wrong, reply];
}
