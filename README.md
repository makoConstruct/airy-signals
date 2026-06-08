Current status: *I'm pretty sure this will work, but haven't implemented it for real yet. I'm going to try using autodispose for a while and seeing if problems really come up.*

# airy signals

Airy Signals are a way of succinctly building data dependency relationships. Their innovation is that they don't require "autodispose" (which can lead to accidental transient zero disposes), and can instead mostly be handled by the GC. They're slightly less efficient than the already very inefficient autodisposing signal, but they're simpler to use and more reliable.

```python
a = Signal(1)
b = Signal(2)
c = Computed(()-> a.use + b.use)
e = Effect(()-> print(c.use))
# > 3
a.value = 2
# > 4
b.value = 3
# > 5
e.dispose()
# at this point, the gc can collect a, b, c and e
```

## Background

Signals are a very good paradigm for data changing over time. They're usually better than Streams or Observables, due to allowing lazy subscriptions (which solves exponential event blowup from long braids of Computed signals), but not requiring them, also allowing Effects. Prior to this work, (non-airy) Signals either had to be disposed manually, or else they would follow an "autodispose" behavior where they'll be collected after gaining one subscriber then after later losing all subscribers, thus introducing a tricky kind of bug where a signal can enter an error state if its subscribers, however transiently, hit 0, in the process of being replaced.

---

Airy signals improve on traditional reactive monads by making Signals and Computeds subject to ordinary garbage collection. They require a finalize() method, which is called by the gc when an object is disposed.

Effects still need to be disposed manually, but this can mostly be abstracted away with advanced paradigms. For instance, a UI component that takes an Airy Signal (like, `SignalBuilder(ReadSignal[T], T -> View)`) can manage the effect automatically as state, disposing it when the component is no longer visible.

**A psuedocode sketch of an implementation:**

```python
# (if you're in a functional context, replace "global" with "contextual parameter")
type SignalGlobals
	# tracks the current signal (computed or effect) that's running, so that subscriptions can be made automatically when use is called within the effect's callback
	scopes: List[SignalNode]
	# the following two can be optimized as intrusive arrays, I believe
	# this one just pins all effects, they need to be manually disposed
	effects: OrderedSet[Effect]
	# the effects to be reevaluated after the current batch of mutations finishes (batching and run scheduling is out of scope for this article)
	nextRunBatch: OrderedSet[SignalNode]
global globals:SignalGlobals

# could represent a signal, computed or effect. This is the part of the signal that may or may not propagate strong refs up and down stream to other SignalNodes. Needs to be a delegate so that the Signal outer can be collected without the SignalNode necessarily being collected, and so that the Signal's finalize can run cleanup on the in-tact structure of the SignalNode.
type SignalNode[T]
	# the SignalNode should maintain strong links only while it is a Root, or while there are Roots, ie, places where the outside world can affect or be affected by the signal chain, *both* upstream and downstream of the node. ie, there must be gc-reachable nodes upstream, or there must be gc-reachable nodes or effects downstream (since effects are gc-reachable, we just use gc-reachability.)
	# This requirement for them to be both upstream and downstream is what puts signal management slightly beyond the domain of conventional garbage collectors, but one can imagine garbage collectors being extended to handle them well, one day. However, since signal networks are acyclic, we don't strictly need garbage collection to handle them (this is also suggestive that it may be possible to implement a decent signals library for Rust)
	# (the reason we need to keep a chain from gc-reachable to gc-reachable even if it has no effects at its end, is that the chain with a gc-accessible end might be connected to effects later, so the user will be very disappointed if the chain body has been disposed before then.)
	rooted: bool = true
	isRoot: bool = true
		set(n) if n == false checkRoots()
	dirty: bool = false
	subscribers: OrderedSet[WeakRef[Signal]]
	_value: T
	previousValue: T?
	upstream: OrderedSet[SignalNode]
	downstream: OrderedSet[SignalNode]
	evaluator: ()->T?
	constructor({this.evaluator})
	get value:T
		if dirty
			reevaluate()
		_value
	fn checkRoots()	
		# this can be optimised so that it doesn't have to iterate all subscribers and fully recompute this, see appendix
		rooted = isRoot or (
		 	upstream.any((r)-> r.rooted) and
			downstream.any((r)-> r.rooted)
		)
		if not rooted
			# delete self 
			# notify neighbors
			for s in upstream
				s.downstream.remove(this)
				s.checkRoots()
			for s in downstream
				s.upstream.remove(this)
				s.checkRoots()
			upstream = []
			downstream = []
	fn mark_dirty()
		if dirty return
		dirty = true
		if evaluator != null
			globals.nextRunBatch.add(this)
		for s in downstream s.mark_dirty()
	# also called when this is scheduled for a run
	fn reevaluate()
		globals.scopes.add(this)
		_value = evaluator()
		globals.scopes.pop()
		diff(upstream, newUpstream, removals:()->
			# unsubscribe and have them checkRoots
			... todo
		)
		upstream = newUpstream
		dirty = false


type SignalNodeBearer[T]
	n: SignalNode[T]
	get value: T n.value
	get use: T
		val scope = globals.scopes.last
		if scope != null
			scope.upstream.add(n)
			n.downstream.add(scope)
		value
	fn Object.finalize()
		n.isRoot = false

type Signal[T] extends SignalNodeBearer[T]
	set v(nv:T)
		n.previousValue = n.value
		n.value = nv
		n.mark_dirty()
		for s in n.downstream s.mark_dirty()
	constructor(n.value)

type Computed[T] extends SignalNodeBearer[T]
	constructor(n.evaluator)
		n.reevaluate()

type Effect extends SignalNodeBearer[Unit]
	constructor(n.evaluator)
		globals.effects.add(this)
		n.reevaluate()
	dispose()
		globals.effects.remove(this)

```


todo:
[ ] complete unsubscription in reevaluation
[ ] contemplate rust implementation
[ ] think about cycle detection
[ ] think more about batching/non-batching
[ ] do a real dart implementation
[ ] talk about optimizations in the appendix
