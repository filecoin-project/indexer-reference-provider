package engine

import (
	"context"
	"encoding/base64"
	"fmt"
	"sync"

	"github.com/filecoin-project/go-legs"
	"github.com/filecoin-project/go-legs/dtsync"
	"github.com/filecoin-project/go-legs/httpsync"
	provider "github.com/filecoin-project/index-provider"
	"github.com/filecoin-project/index-provider/cardatatransfer"
	"github.com/filecoin-project/index-provider/engine/chunker"
	stiapi "github.com/filecoin-project/storetheindex/api/v0"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/hashicorp/go-multierror"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dsn "github.com/ipfs/go-datastore/namespace"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

const (
	keyToCidMapPrefix      = "map/keyCid/"
	cidToKeyMapPrefix      = "map/cidKey/"
	keyToMetadataMapPrefix = "map/keyMD/"
	latestAdvKey           = "sync/adv/"
	linksCachePath         = "/cache/links"
)

var (
	log = logging.Logger("provider/engine")

	dsLatestAdvKey = datastore.NewKey(latestAdvKey)
)

// Engine is an implementation of the core reference provider interface
type Engine struct {
	*options
	lsys ipld.LinkSystem

	entriesChunker *chunker.CachedEntriesChunker

	publisher legs.Publisher

	// cb is the callback used in the linkSystem
	cb   provider.Callback
	cblk sync.Mutex
}

var _ provider.Interface = (*Engine)(nil)

// New creates a new index provider Engine as the default implementation of provider.Interface.
// It provides the ability to advertise the availability of a list of multihashes associated to
// a context ID as a chain of linked advertisements as defined by the indexer node protocol implemented by "storetheindex".
// Engine internally uses "go-legs", a protocol for propagating and synchronizing changes an IPLD DAG, to publish advertisements.
// See:
//  - https://github.com/filecoin-project/storetheindex
//  - https://github.com/filecoin-project/go-legs
//
// Published advertisements are signed using the given private key.
// The retAddrs corresponds to the endpoints at which the data block associated to the advertised
// multihashes can be retrieved.
// Note that if no retAddrs is specified the listen addresses of the given libp2p host are used.
//
// The engine also provides the ability to generate advertisements via Engine.NotifyPut and
// Engine.NotifyRemove as long as a provider.Callback is registered.
// See: provider.Callback, Engine.RegisterCallback.
//
// The engine must be started via Engine.Start before use and discarded via Engine.Shutdown when no longer needed.
// See: Engine.Start, Engine.Shutdown.
func New(o ...Option) (*Engine, error) {
	opts, err := newOptions(o...)
	if err != nil {
		return nil, err
	}

	e := &Engine{
		options: opts,
	}

	e.lsys = e.mkLinkSystem()

	return e, nil
}

// Start starts the engine by instantiating the internal storage and joins the configured gossipsub
// topic used for publishing advertisements.
//
// The context is used to instantiate the internal LRU cache storage.
//
// See: Engine.Shutdown, chunker.NewCachedEntriesChunker, dtsync.NewPublisherFromExisting.
func (e *Engine) Start(ctx context.Context) error {
	// Create datastore entriesChunker
	entriesCacheDs := dsn.Wrap(e.ds, datastore.NewKey(linksCachePath))
	cachedChunker, err := chunker.NewCachedEntriesChunker(ctx, entriesCacheDs, e.entChunkSize, e.entCacheCap)
	if err != nil {
		return err
	}

	if e.purgeCache {
		err := cachedChunker.Clear(ctx)
		if err != nil {
			return err
		}
	}

	e.entriesChunker = cachedChunker

	e.publisher, err = e.newPublisher()
	if err != nil {
		log.Errorw("Failed to instantiate legs publisher", "err", "err", "kind", e.pubKind)
		return err
	}

	err = e.PublishLatest(ctx)
	if err != nil {
		log.Errorw("Could not republish latest advertisement", "err", err)
		return err
	}

	return nil
}

func (e *Engine) newPublisher() (legs.Publisher, error) {
	switch e.pubKind {
	case NoPublisher:
		log.Info("Remote announcements is disabled; all advertisements will only be store locally.")
		return nil, nil
	case DataTransferPublisher:
		dtOpts := []dtsync.Option{dtsync.Topic(e.pubTopic), dtsync.WithExtraData(e.pubExtraGossipData)}
		if e.pubDT != nil {
			return dtsync.NewPublisherFromExisting(e.pubDT, e.h, e.pubTopicName, e.lsys, dtOpts...)
		}
		ds := dsn.Wrap(e.ds, datastore.NewKey("/legs/dtsync/pub"))
		return dtsync.NewPublisher(e.h, ds, e.lsys, e.pubTopicName, dtOpts...)
	case HttpPublisher:
		return httpsync.NewPublisher(e.pubHttpListenAddr, e.lsys, e.h.ID(), e.key)
	default:
		return nil, fmt.Errorf("unknown publisher kind: %s", e.pubKind)
	}
}

// PublishLocal stores the advertisement in the local link system and marks it locally as the latest
// advertisement.
//
// The context is used for storing internal mapping information onto the datastore.
//
// See: Engine.Publish.
func (e *Engine) PublishLocal(ctx context.Context, adv schema.Advertisement) (cid.Cid, error) {
	lnk, err := e.lsys.Store(ipld.LinkContext{Ctx: ctx}, schema.Linkproto, adv.Representation())
	if err != nil {
		return cid.Undef, fmt.Errorf("cannot generate advertisement link: %s", err)
	}
	c := lnk.(cidlink.Link).Cid
	log := log.With("adCid", c)
	log.Info("Stored ad in local link system")

	if err := e.putLatestAdv(ctx, c.Bytes()); err != nil {
		log.Errorw("Failed to update reference to the latest advertisement", "err", err)
		return cid.Undef, fmt.Errorf("failed to update reference to latest advertisement: %w", err)
	}
	log.Info("Updated reference to the latest advertisement successfully")
	return c, nil
}

// Publish stores the given advertisement locally via Engine.PublishLocal first, then publishes
// a message onto the gossipsub to signal the change in the latest advertisement by the provider to
// indexer nodes.
//
// The publication mechanism uses legs.Publisher internally.
// See: https://github.com/filecoin-project/go-legs
func (e *Engine) Publish(ctx context.Context, adv schema.Advertisement) (cid.Cid, error) {
	c, err := e.PublishLocal(ctx, adv)
	if err != nil {
		log.Errorw("Failed to store advertisement locally", "err", err)
		return cid.Undef, fmt.Errorf("failed to publish advertisement locally: %w", err)
	}

	// Only announce the advertisement CID if publisher is configured.
	if e.publisher != nil {
		log := log.With("adCid", c)
		log.Info("Publishing advertisement in pubsub channel")
		err = e.publisher.UpdateRoot(ctx, c)
		if err != nil {
			log.Errorw("Failed to announce advertisement on pubsub channel ", "err", err)
			return cid.Undef, err
		}
	}
	return c, nil
}

// PublishLatest re-publishes the latest existing advertisement to pubsub.
func (e *Engine) PublishLatest(ctx context.Context) error {
	// Skip announcing the latest advertisement CID if there is no publisher.
	if e.publisher == nil {
		log.Infow("Skipped announcing the latest: remote announcements are disabled.")
		return nil
	}

	adCid, err := e.getLatestAdCid(ctx)
	if err != nil {
		log.Errorw("Failed to get the latest advertisement CID", "err", err)
		return fmt.Errorf("failed to get latest advertisement cid from blockstore: %w", err)
	}

	if adCid == cid.Undef {
		log.Info("Skipped announcing the latest: no previously published advertisements.")
		return nil
	}
	log.Infow("Republishing latest advertisement", "cid", adCid)

	return e.publisher.UpdateRoot(ctx, adCid)
}

// RegisterCallback registers a provider.Callback that is used to look up the
// list of multihashes associated to a context ID. At least one such callback
// must be registered before calls to Engine.NotifyPut and Engine.NotifyRemove.
//
// Note that successive calls to this function will replace the previous callback.
// Only a single callback is supported.
//
// See: provider.Interface
func (e *Engine) RegisterCallback(cb provider.Callback) {
	log.Debugf("Registering callback in engine")
	e.cblk.Lock()
	defer e.cblk.Unlock()
	e.cb = cb
}

// NotifyPut publishes an advertisement that signals the list of multihashes
// associated to the given contextID is available by this provider with the
// given metadata. A provider.Callback is required, and is used to look up the
// list of multihashes associated to a context ID.
//
// Note that prior to calling this function a provider.Callback must be registered.
//
// See: Engine.RegisterCallback, Engine.Publish.
func (e *Engine) NotifyPut(ctx context.Context, contextID []byte, metadata stiapi.Metadata) (cid.Cid, error) {
	// The callback must have been registered for the linkSystem to know how to
	// go from contextID to list of CIDs.
	return e.publishAdvForIndex(ctx, contextID, metadata, false)
}

// NotifyRemove publishes an advertisement that signals the list of multihashes associated to the given
// contextID is no longer available by this provider.
//
// Note that prior to calling this function a provider.Callback must be registered.
//
// See: Engine.RegisterCallback, Engine.Publish.
func (e *Engine) NotifyRemove(ctx context.Context, contextID []byte) (cid.Cid, error) {
	return e.publishAdvForIndex(ctx, contextID, stiapi.Metadata{}, true)
}

// Shutdown shuts down the engine and discards all resources opened by the engine.
// The engine is no longer usable after the call to this function.
func (e *Engine) Shutdown() error {
	var errs error
	if e.publisher != nil {
		if err := e.publisher.Close(); err != nil {
			errs = multierror.Append(errs, fmt.Errorf("error closing leg publisher: %s", err))
		}
	}
	if err := e.entriesChunker.Close(); err != nil {
		errs = multierror.Append(errs, fmt.Errorf("error closing link entriesChunker: %s", err))
	}
	return errs
}

// GetAdv gets the advertisement associated to the given cid c.
// The context is not used.
func (e *Engine) GetAdv(_ context.Context, adCid cid.Cid) (schema.Advertisement, error) {
	log := log.With("cid", adCid)
	log.Infow("Getting advertisement by CID")

	l, err := schema.LinkAdvFromCid(adCid).AsLink()
	if err != nil {
		return nil, fmt.Errorf("cannot convert cid to advertisement link: %s", err)
	}

	lsys := e.vanillaLinkSystem()
	n, err := lsys.Load(ipld.LinkContext{}, l, schema.Type.Advertisement)
	if err != nil {
		return nil, fmt.Errorf("cannot load advertisement from blockstore with vanilla linksystem: %s", err)
	}
	adv, ok := n.(schema.Advertisement)
	if !ok {
		return nil, fmt.Errorf("stored IPLD node for cid is not an advertisement")
	}
	return adv, nil
}

// GetLatestAdv gets the latest advertisement by the provider.  If there are
// not previously published advertisements, then cid.Undef is returned as the
// advertisement CID.
func (e *Engine) GetLatestAdv(ctx context.Context) (cid.Cid, schema.Advertisement, error) {
	log.Info("Getting latest advertisement")
	latestAdCid, err := e.getLatestAdCid(ctx)
	if err != nil {
		return cid.Undef, nil, fmt.Errorf("could not get latest advertisement cid from blockstore: %s", err)
	}
	if latestAdCid == cid.Undef {
		return cid.Undef, nil, nil
	}

	ad, err := e.GetAdv(ctx, latestAdCid)
	if err != nil {
		return cid.Undef, nil, fmt.Errorf("count not get latest advertisement from blockstore by cid: %s", err)
	}
	return latestAdCid, ad, nil
}

func (e *Engine) publishAdvForIndex(ctx context.Context, contextID []byte, metadata stiapi.Metadata, isRm bool) (cid.Cid, error) {
	var err error
	var cidsLnk cidlink.Link

	log := log.With("contextID", base64.StdEncoding.EncodeToString(contextID))

	c, err := e.getKeyCidMap(ctx, contextID)
	if err != nil {
		if err != datastore.ErrNotFound {
			return cid.Undef, fmt.Errorf("cound not not get entries cid by context id: %s", err)
		}
	}

	// If we are not removing, we need to generate the link for the list
	// of CIDs from the contextID using the callback, and store the relationship
	if !isRm {
		log.Info("Creating advertisement")

		// If no previously-published ad for this context ID.
		if c == cid.Undef {
			log.Info("Generating entries linked list for advertisement")
			// If no callback registered return error
			if e.cb == nil {
				return cid.Undef, provider.ErrNoCallback
			}

			// Call the callback
			mhIter, err := e.cb(ctx, contextID)
			if err != nil {
				return cid.Undef, err
			}
			// Generate the linked list ipld.Link that is added to the
			// advertisement and used for ingestion.
			lnk, err := e.entriesChunker.Chunk(ctx, mhIter)
			if err != nil {
				return cid.Undef, fmt.Errorf("could not generate entries list: %s", err)
			}
			cidsLnk = lnk.(cidlink.Link)

			// Store the relationship between contextID and CID of the advertised
			// list of Cids.
			err = e.putKeyCidMap(ctx, contextID, cidsLnk.Cid)
			if err != nil {
				return cid.Undef, fmt.Errorf("failed to write context id to entries cid mapping: %s", err)
			}
		} else {
			// Lookup metadata for this contextID.
			prevMetadata, err := e.getKeyMetadataMap(ctx, contextID)
			if err != nil {
				if err != datastore.ErrNotFound {
					return cid.Undef, fmt.Errorf("could not get metadata for context id: %s", err)
				}
				log.Warn("No metadata for existing context ID, generating new advertisement")
			}

			if metadata.Equal(prevMetadata) {
				// Metadata is the same; no change, no need for new advertisement.
				return cid.Undef, provider.ErrAlreadyAdvertised
			}

			// Linked list is the same, but metadata is different, so generate
			// new advertisement with same linked list, but new metadata.
			cidsLnk = cidlink.Link{Cid: c}
		}

		if err = e.putKeyMetadataMap(ctx, contextID, metadata); err != nil {
			return cid.Undef, fmt.Errorf("failed to write context id to metadata mapping: %s", err)
		}
	} else {
		log.Info("Creating removal advertisement")

		if c == cid.Undef {
			return cid.Undef, provider.ErrContextIDNotFound
		}

		// And if we are removing it means we probably do not have the list of
		// CIDs anymore, so we can remove the entry from the datastore.
		err = e.deleteKeyCidMap(ctx, contextID)
		if err != nil {
			return cid.Undef, fmt.Errorf("failed to delete context id to entries cid mapping: %s", err)
		}
		err = e.deleteCidKeyMap(ctx, c)
		if err != nil {
			return cid.Undef, fmt.Errorf("failed to delete entries cid to context id mapping: %s", err)
		}
		err = e.deleteKeyMetadataMap(ctx, contextID)
		if err != nil {
			return cid.Undef, fmt.Errorf("failed to delete context id to metadata mapping: %s", err)
		}

		// Create an advertisement to delete content by contextID by specifying
		// that advertisement has no entries.
		cidsLnk = schema.NoEntries

		// The advertisement still requires a valid metadata even though
		// metadata is not used for removal.  Create a valid empty metadata.
		metadata = stiapi.Metadata{
			ProtocolID: cardatatransfer.ContextIDCodec,
		}
	}

	// Get the previous advertisement that was generated
	prevAdvID, err := e.getLatestAdCid(ctx)
	if err != nil {
		return cid.Undef, fmt.Errorf("could not get latest advertisement: %s", err)
	}
	var previousLnk schema.Link_Advertisement
	// Check for cid.Undef for the previous link. If this is the case, then
	// this means there is a "cid too short" error in IPLD links serialization.
	if prevAdvID == cid.Undef {
		log.Info("Latest advertisement CID was undefined - no previous advertisement")
		previousLnk = nil
	} else {
		nb := schema.Type.Link_Advertisement.NewBuilder()
		err = nb.AssignLink(cidlink.Link{Cid: prevAdvID})
		if err != nil {
			return cid.Undef, fmt.Errorf("failed to generate link from latest advertisement cid: %s", err)
		}
		previousLnk = nb.Build().(schema.Link_Advertisement)
	}

	adv, err := schema.NewAdvertisement(e.key, previousLnk, cidsLnk,
		contextID, metadata, isRm, e.h.ID().String(), e.retrievalAddrsAsString())
	if err != nil {
		return cid.Undef, fmt.Errorf("failed to create advertisement: %s", err)
	}
	return e.Publish(ctx, adv)
}

func (e *Engine) putKeyCidMap(ctx context.Context, contextID []byte, c cid.Cid) error {
	// We need to store the map Key-Cid to know what CidLink to put
	// in advertisement when we notify a removal.
	err := e.ds.Put(ctx, datastore.NewKey(keyToCidMapPrefix+string(contextID)), c.Bytes())
	if err != nil {
		return err
	}
	// And the other way around when graphsync ios making a request,
	// so the callback in the linksystem knows to what contextID we are referring.
	return e.ds.Put(ctx, datastore.NewKey(cidToKeyMapPrefix+c.String()), contextID)
}

func (e *Engine) getKeyCidMap(ctx context.Context, contextID []byte) (cid.Cid, error) {
	b, err := e.ds.Get(ctx, datastore.NewKey(keyToCidMapPrefix+string(contextID)))
	if err != nil {
		return cid.Undef, err
	}
	_, d, err := cid.CidFromBytes(b)
	return d, err
}

func (e *Engine) deleteKeyCidMap(ctx context.Context, contextID []byte) error {
	return e.ds.Delete(ctx, datastore.NewKey(keyToCidMapPrefix+string(contextID)))
}

func (e *Engine) deleteCidKeyMap(ctx context.Context, c cid.Cid) error {
	return e.ds.Delete(ctx, datastore.NewKey(cidToKeyMapPrefix+c.String()))
}

func (e *Engine) getCidKeyMap(ctx context.Context, c cid.Cid) ([]byte, error) {
	return e.ds.Get(ctx, datastore.NewKey(cidToKeyMapPrefix+c.String()))
}

func (e *Engine) putKeyMetadataMap(ctx context.Context, contextID []byte, metadata stiapi.Metadata) error {
	data, err := metadata.MarshalBinary()
	if err != nil {
		return err
	}
	return e.ds.Put(ctx, datastore.NewKey(keyToMetadataMapPrefix+string(contextID)), data)
}

func (e *Engine) getKeyMetadataMap(ctx context.Context, contextID []byte) (stiapi.Metadata, error) {
	data, err := e.ds.Get(ctx, datastore.NewKey(keyToMetadataMapPrefix+string(contextID)))
	if err != nil {
		return stiapi.Metadata{}, err
	}
	var metadata stiapi.Metadata
	if err = metadata.UnmarshalBinary(data); err != nil {
		return stiapi.Metadata{}, err
	}
	return metadata, nil
}

func (e *Engine) deleteKeyMetadataMap(ctx context.Context, contextID []byte) error {
	return e.ds.Delete(ctx, datastore.NewKey(keyToMetadataMapPrefix+string(contextID)))
}

func (e *Engine) putLatestAdv(ctx context.Context, advID []byte) error {
	return e.ds.Put(ctx, dsLatestAdvKey, advID)
}

func (e *Engine) getLatestAdCid(ctx context.Context) (cid.Cid, error) {
	b, err := e.ds.Get(ctx, dsLatestAdvKey)
	if err != nil {
		if err == datastore.ErrNotFound {
			return cid.Undef, nil
		}
		return cid.Undef, err
	}
	_, c, err := cid.CidFromBytes(b)
	return c, err
}
