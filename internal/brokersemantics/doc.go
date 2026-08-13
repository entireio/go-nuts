// Package brokersemantics is the real-broker test suite: every JetStream
// semantic this module's code and documentation rely on, asserted against an
// embedded in-process nats-server at the version pinned in go.mod.
//
// # Why this exists
//
// Every P1 found in the A1 review cycle was a model error — code faithfully
// implementing a wrong belief about JetStream. The unit suites run against
// natsmsgtest.FakeMsg, which encodes the same model as the code, so it agrees
// with the code exactly when the code is wrong: Term was believed not to settle
// (ENT-1492); NumDelivered was read as a ladder counter the handler drives;
// server-side BackOff was believed to govern Nak delays; the last BackOff rung's
// tail-repeat arithmetic was off by one (COR-1255). A fake cannot catch any of
// those, and the only step in the pipeline that consulted reality was human
// review — which is why review kept finding P1s (COR-1257).
//
// So the division of labour is:
//
//   - natsmsgtest.FakeMsg — library logic only: did this code call Ack, or Nak
//     with this delay, given this input? Disposition bookkeeping, not semantics.
//   - this suite — every belief about what the broker DOES: settlement, ack-floor
//     movement, delivery counting, ladder arithmetic, config normalization and
//     rejection. If a doc comment in this module states a JetStream behaviour, a
//     test here measures it.
//
// The suite deliberately carries no build tag: it runs in the default
// `go test ./...`, so it gates every merge. A tag CI forgets to pass is a gate
// that silently does not run — the same failure shape as a fake that agrees
// with the code.
//
// # Fixtures that cannot fail
//
// A real broker is necessary but not sufficient: a fixture whose parameters are
// degenerate measures the semantic away, and then agrees with the code for the
// same reason a fake does.
//
// The worked example is TestBackOffGovernsAckTimeoutsNotNakDelays. A NakWithDelay
// is served after d + (BackOff[rung] - BackOff[0]), so on a FLAT ladder — every
// rung equal — the stretch term is exactly zero and the requested delay appears to
// be honoured. Any fixture built on 3s/3s/3s therefore passes whether the stretch
// exists or not, against a live server, while pinning nothing. Only a GROWING
// ladder can fail.
//
// So when adding or re-verifying a case here, pick values that would come out
// differently under the belief being rejected, and prefer asserting a measured
// quantity (a gap, a sequence, a stored config field) over asserting that a call
// returned no error — several of the semantics below are precisely calls that
// return no error while doing nothing.
//
// For a TIMING assertion there is a second rule, because the expected value being
// right is not enough: the tolerance has to be narrower than the distance to the
// next plausible explanation. A ±500ms band around a 150ms ladder rung accepts the
// 400ms rung beside it, so the case passes under precisely the off-by-one it was
// written to reject. [assertGap] enforces this rather than trusting the author —
// it takes the rival delays (adjacent rungs, the bare AckWait, the un-stretched
// request), tightens the band to half the nearest rival's distance, and fails any
// fixture whose delays sit too close to be told apart. When it does, the fixture's
// values are what change; widening the band is how the assertion stops working.
//
// Then PROVE the case can fail, by breaking the thing it claims to gate and
// watching it go red: raise the cap, grant the permission, silence one
// disposition, or assert the rival timing and watch the real one contradict it.
// Review has now caught four fixtures here that passed against a live broker while
// gating nothing — a heartbeat test that stopped the heartbeat itself, so deleting
// the cap left it green; a permission test that exercised only Ack while the
// contract promised the same silence for Nak and Term; a concurrent attribution
// that matched a durable as a bare substring, letting denied_nakdelay's violation
// satisfy denied_nak's assertion; and gap bands wider than the spacing between the
// rungs they were distinguishing, which left both the tail-repeat arithmetic and
// the NakWithDelay stretch — the suite's two sharpest findings — unguarded. None
// of those was a wrong assertion. Each was an assertion nothing could break, which
// is the same defect as the fake and costs the same amount to find: one review
// round each.
//
// # On a nats-server bump
//
// These semantics are version-specific, so the suite re-runs on every server
// bump: that is how a semantic change is discovered by a red test instead of by
// an incident.
//
// It also settles which version a belief was ever true on. ENT-1492 and COR-944
// record that "a Term does NOT delete it (verified live on NATS 2.14.2)", and
// the fleet stream manifests still carry that note. Running
// TestTermSettlesAndAdvancesAckFloor against an embedded 2.14.2 as well as the
// pinned version produces identical results — Term settles immediately on both,
// on all three retention policies — so that belief does not reproduce on either
// version single-node, and prod's floor pin has another cause. The nearest
// mechanism this suite does reproduce is
// TestAckWithoutPublishPermissionSilentlySucceeds: a Term whose $JS.ACK publish
// is denied returns no error at all, leaving the floor pinned with nothing
// logged (COR-1224 found five consumers shipped without that grant).
//
// Failures name the version they were measured against (see
// TestPinnedServerVersion): when a test here goes red after a bump, the belief in
// the doc comment it cites — not the test — is what needs re-deciding.
package brokersemantics
