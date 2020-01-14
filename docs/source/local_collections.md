# Local collections
Local collections are designed to govern dissemination in a way that is entirely proposal-driven. The existing collections operate on the basis of configuration: that is, preimage data is disseminated to peers who are entitled, and entitlement is determined based on configuration data that is stored on the ledger, in the lifecycle metadata.
This approach has the disadvantage of leaking collection membership by correlating the collection name with the collection configuration data, and extracting the identity of the peers involved in the transaction by deriving it from the dissemination policy.
Local collections address this issue by having no such configuration data: a peer possesses a preimage if it generated it during chaincode simulation.
The flow proceeds as follows:
1) a client sends a transaction proposal message to a set of peers; any confidential data is stored in the transient field
2) the peers simulate the chaincode execution and perform private data puts. Preimages are stored in the transient DB and may optionally be disseminated to other peers of the same organisation.
3) the transaction eventually commits. Peers inspect the transient DB to determine whether they possess the preimage. If they do, they persist it in the private DB and will be able to simulate future chaincode invocations that require that preimage.

## Usage

The current implementation automatically creates a collection called `+local`. Any `PutPrivateData` operation that uses that string as the collection name will execute correctly, and will perist the preimage locally to that peer, without leaking collection membership data.
Equally, `GetPrivateData` will accept the `+local` collection name and will return successfully if the peer has received the preimage.

## Shortcomings

The current implementation has the following known shortcomings:
* dissemination is _only_ proposal-based. If an organisation is supposed to have a preimage, then the client must collect endorsements from _all_ peers of that organisation before submitting the transaction for ordering. Future versions of local collections will integrate dissemination similar to the one implemented for current collections, but one that doesn't break the privacy of the involved organisations.
* while collection configuration data no longer leaks the identity of the involved organisations, this information may still be gathered by observing the endorsement field. This leakage is orthogonal to collections and may be plugged using privacy-preserving endorsements.
