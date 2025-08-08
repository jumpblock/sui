use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};
use futures::future::BoxFuture;
use crate::authority::authority_per_epoch_store::AuthorityPerEpochStore;
use crate::authority::authority_store::{SuiLockResult};
use crate::execution_cache::{ExecutionCacheTraitPointers, ObjectCacheRead};
use sui_types::base_types::{FullObjectID, ObjectID, ObjectRef, SequenceNumber, VersionNumber};
use sui_types::bridge::Bridge;
use sui_types::clock::Clock;
use sui_types::committee::EpochId;
use sui_types::digests::TransactionDigest;
use sui_types::error::{SuiError, SuiResult, UserInputError};
use sui_types::id::{ID, UID};
use sui_types::messages_checkpoint::CheckpointSequenceNumber;
use sui_types::object::{Data, MoveObject, Object, ObjectInner, Owner, OBJECT_START_VERSION};
use sui_types::storage::{FullObjectKey, InputKey, MarkerValue, ObjectKey, ObjectOrTombstone, PackageObject};
use sui_types::sui_system_state::SuiSystemState;
use sui_types::transaction::{ObjectReadResultKind};
use sui_types::SUI_CLOCK_OBJECT_ID;
use sui_types::{
    storage::{BackingPackageStore, ChildObjectResolver, ObjectStore, ParentSync},
    transaction::ObjectReadResult,
};

fn latest_clock_object()->anyhow::Result<Object>{
    let mo=unsafe{
        MoveObject::new_from_execution_with_limit(
            Clock::type_().into(),
            false,
            OBJECT_START_VERSION,
            bcs::to_bytes(&Clock {
                id: UID {
                    id: ID {
                        bytes: SUI_CLOCK_OBJECT_ID,
                    },
                },
                timestamp_ms: { SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64 },
            })?,
            u64::MAX,
        )?
    };

    Ok(ObjectInner {
        owner: Owner::Shared {
            initial_shared_version: OBJECT_START_VERSION,
        },
        data: Data::Move(mo),
        previous_transaction: TransactionDigest::genesis_marker(),
        storage_rebate: 0,
    }
        .into())
}

pub struct OverrideCache {
    pub fallback: ExecutionCacheTraitPointers,
    pub overrides: HashMap<ObjectID,ObjectReadResult>
}

impl OverrideCache {
    pub fn new(fallback: ExecutionCacheTraitPointers, list: Vec<ObjectReadResult>) -> Self {
        let mut overrides: HashMap<ObjectID,ObjectReadResult>=HashMap::with_capacity(list.len());
        list.into_iter().for_each(|o|{
            overrides.insert(o.id(),o);
        });
        Self {
            fallback,
            overrides,
        }
    }

    pub fn get_override(&self, object_id: &ObjectID) -> Option<ObjectReadResult> {
        if object_id == &SUI_CLOCK_OBJECT_ID {
            return Some(ObjectReadResult {
                input_object_kind: sui_types::transaction::InputObjectKind::SharedMoveObject {
                    id: SUI_CLOCK_OBJECT_ID,
                    initial_shared_version: OBJECT_START_VERSION,
                    mutable: true,
                },
                object: ObjectReadResultKind::Object(latest_clock_object().ok()?),
            });
        }

        // self.overrides.iter().find(|o| o.id() == *object_id).cloned()
        self.overrides.get(object_id).cloned()
    }

    pub fn get_override_object(&self, object_id: &ObjectID) -> Option<Object> {
        match self.get_override(object_id) {
            Some(r) => match r.object {
                ObjectReadResultKind::Object(object) => Some(object),
                ObjectReadResultKind::ObjectConsensusStreamEnded(_, _) => None,
                ObjectReadResultKind::CancelledTransactionSharedObject(_) => unreachable!("override object is in cancelled transaction"),
            },
            _ => None,
        }
    }
}

impl BackingPackageStore for OverrideCache {
    fn get_package_object(&self, package_id: &ObjectID) -> SuiResult<Option<PackageObject>> {
        ObjectCacheRead::get_package_object(self, package_id)
    }
}

impl ChildObjectResolver for OverrideCache {
    fn read_child_object(
        &self,
        parent: &ObjectID,
        child: &ObjectID,
        child_version_upper_bound: SequenceNumber,
    ) -> SuiResult<Option<Object>> {
        let Some(child_object) = self.find_object_lt_or_eq_version(*child, child_version_upper_bound) else {
            return Ok(None);
        };

        let parent = *parent;
        if child_object.owner != Owner::ObjectOwner(parent.into()) {
            let sui_error = SuiError::InvalidChildObjectAccess {
                object: *child,
                given_parent: parent,
                actual_owner: child_object.owner.clone(),
            };
            return Err(sui_error);
        }
        Ok(Some(child_object))
    }

    fn get_object_received_at_version(
        &self,
        owner: &ObjectID,
        receiving_object_id: &ObjectID,
        receive_object_at_version: SequenceNumber,
        epoch_id: EpochId,
    ) -> SuiResult<Option<Object>> {
        let Some(recv_object) =
            ObjectCacheRead::get_object_by_key(self, receiving_object_id, receive_object_at_version)
        else {
            return Ok(None);
        };

        // Check for:
        // * Invalid access -- treat as the object does not exist. Or;
        // * If we've already received the object at the version -- then treat it as though it doesn't exist.
        // These two cases must remain indisguishable to the caller otherwise we risk forks in
        // transaction replay due to possible reordering of transactions during replay.
        if recv_object.owner != Owner::AddressOwner((*owner).into())
            || self.have_received_object_at_version(FullObjectKey::Fastpath(ObjectKey(*receiving_object_id, receive_object_at_version)), epoch_id)
        {
            return Ok(None);
        }

        Ok(Some(recv_object))
    }
}
impl ObjectStore for OverrideCache {
    fn get_object(&self, object_id: &ObjectID) -> Option<Object> {
        ObjectCacheRead::get_object(self, object_id)
    }

    fn get_object_by_key(&self, object_id: &ObjectID, version: VersionNumber) -> Option<Object> {
        ObjectCacheRead::get_object_by_key(self, object_id, version)
    }
}
impl ParentSync for OverrideCache {
    fn get_latest_parent_entry_ref_deprecated(&self, _id: ObjectID) -> Option<ObjectRef> {
        panic!("won't be called in new version")
    }
}

impl ObjectCacheRead for OverrideCache {
    fn get_package_object(&self, id: &ObjectID) -> SuiResult<Option<PackageObject>> {
        // first check if the object is in the overrides
        if let Some(override_object) = self.get_override_object(id) {
            return Ok(Some(PackageObject::new(override_object)));
        };

        self.fallback.object_cache_reader.get_package_object(id)
    }

    fn force_reload_system_packages(&self, system_package_ids: &[ObjectID]) {
        self.fallback.object_cache_reader.force_reload_system_packages(system_package_ids)
    }

    fn get_object(&self, id: &ObjectID) -> Option<Object> {
        // first check if the object is in the overrides
        if let Some(override_object) = self.get_override_object(id) {
            return Some(override_object);
        };

        self.fallback.object_cache_reader.get_object(id)
    }

    fn get_latest_object_ref_or_tombstone(&self, object_id: ObjectID) -> Option<ObjectRef> {
        if let Some(override_object) = self.get_override_object(&object_id) {
            return Some(override_object.compute_object_reference());
        }

        // debug!(
        //     "❗️ [get_latest_object_ref_or_tombstone] override missing: {:?}",
        //     object_id
        // );
        // if it's not found, we lookup in fallback
        // if it's deleted, also lookup in fallback because it's not deleted in fallback
        // (we don't have object digest for deleted object in override)
        self.fallback.object_cache_reader.get_latest_object_ref_or_tombstone(object_id)
    }

    fn get_latest_object_or_tombstone(&self, object_id: ObjectID) -> Option<(ObjectKey, ObjectOrTombstone)> {
        if let Some(override_object) = self.get_override(&object_id) {
            match override_object.object {
                ObjectReadResultKind::Object(object) => {
                    return Some((
                        ObjectKey::from(object.compute_object_reference()),
                        ObjectOrTombstone::Object(object),
                    ))
                }
                ObjectReadResultKind::ObjectConsensusStreamEnded(_, _) => {
                    let undeleted_object = match self.fallback.object_cache_reader.get_object(&object_id) {
                        Some(object) => object,
                        // is it possible?
                        None => return None,
                    };

                    let object_ref = undeleted_object.compute_object_reference();
                    return Some((ObjectKey::from(&object_ref), ObjectOrTombstone::Tombstone(object_ref)));
                }
                ObjectReadResultKind::CancelledTransactionSharedObject(_) => {
                    unreachable!("override object is in cancelled transaction")
                }
            }
        }

        self.fallback.object_cache_reader.get_latest_object_or_tombstone(object_id)
    }

    fn get_object_by_key(&self, object_id: &ObjectID, version: SequenceNumber) -> Option<Object> {
        if let Some(override_object) = self.get_override(object_id) {
            match override_object.object {
                ObjectReadResultKind::Object(object) => {
                    if object.version() == version {
                        return Some(object);
                    }
                }
                ObjectReadResultKind::ObjectConsensusStreamEnded(_, _) => return None,
                ObjectReadResultKind::CancelledTransactionSharedObject(_)=> {
                    unreachable!("override object is in cancelled transaction")
                }
            }
        }

        self.fallback.object_cache_reader.get_object_by_key(object_id, version)
    }

    fn multi_get_objects_by_key(&self, object_keys: &[ObjectKey]) -> Vec<Option<Object>> {
        let mut result = vec![];
        for object_key in object_keys {
            result.push((self as &dyn ObjectCacheRead).get_object_by_key(&object_key.0, object_key.1));
        }
        result
    }

    fn object_exists_by_key(&self, object_id: &ObjectID, version: SequenceNumber) -> bool {
        let Some(_) = (self as &dyn ObjectCacheRead).get_object_by_key(object_id, version) else {
            return false;
        };
        true
    }

    fn multi_object_exists_by_key(&self, object_keys: &[ObjectKey]) -> Vec<bool> {
        let mut result = vec![];
        for object_key in object_keys {
            result.push((self as &dyn ObjectCacheRead).object_exists_by_key(&object_key.0, object_key.1));
        }
        result
        // self.fallback.object_cache_reader.multi_object_exists_by_key(object_keys)
    }

    fn multi_input_objects_available_cache_only(&self, keys: &[InputKey]) -> Vec<bool> {
        self.fallback.object_cache_reader.multi_input_objects_available_cache_only(keys)
    }

    fn find_object_lt_or_eq_version(&self, object_id: ObjectID, version: SequenceNumber) -> Option<Object> {
        let object = (self as &dyn ObjectCacheRead).get_object(&object_id)?;
        if object.version() <= version {
            return Some(object);
        }
        self.fallback.object_cache_reader.find_object_lt_or_eq_version(object_id,version)
    }

    fn get_lock(&self, obj_ref: ObjectRef, epoch_store: &AuthorityPerEpochStore) -> SuiLockResult {
        self.fallback.object_cache_reader.get_lock(obj_ref, epoch_store)
    }

    fn _get_live_objref(&self, object_id: ObjectID) -> SuiResult<ObjectRef> {
        // if deleted, return Err
        if let Some(override_object) = self.get_override(&object_id) {
            match override_object.object {
                ObjectReadResultKind::Object(object) => {
                    return Ok(object.compute_object_reference());
                }
                ObjectReadResultKind::ObjectConsensusStreamEnded(_, _) => {
                    return Err(SuiError::UserInputError {
                        error: UserInputError::ObjectNotFound {
                            object_id,
                            version: None,
                        },
                    })
                }
                ObjectReadResultKind::CancelledTransactionSharedObject(_)=> {
                    unreachable!("override object is in cancelled transaction")
                }
            }
        }

        self.fallback.object_cache_reader._get_live_objref(object_id)
    }

    fn check_owned_objects_are_live(&self, owned_object_refs: &[ObjectRef]) -> SuiResult {
        for owned_object_ref in owned_object_refs {
            if (self as &dyn ObjectCacheRead)
                .get_object_by_key(&owned_object_ref.0, owned_object_ref.1)
                .is_none()
            {
                return Err(UserInputError::ObjectVersionUnavailableForConsumption {
                    provided_obj_ref: *owned_object_ref,
                    current_version: owned_object_ref.1,
                }
                .into());
            }
        }

        Ok(())
    }

    fn get_sui_system_state_object_unsafe(&self) -> SuiResult<SuiSystemState> {
        self.fallback.object_cache_reader.get_sui_system_state_object_unsafe()
    }

    fn get_bridge_object_unsafe(&self) -> SuiResult<Bridge> {
        self.fallback.object_cache_reader.get_bridge_object_unsafe()
    }

    fn get_marker_value(
        &self,
        object_key: FullObjectKey,
        epoch_id: EpochId,
    ) -> Option<MarkerValue> {
        // TODO: implement
        self.fallback.object_cache_reader.get_marker_value(object_key, epoch_id)
    }

    fn get_latest_marker(&self, object_id: FullObjectID, epoch_id: EpochId) -> Option<(SequenceNumber, MarkerValue)> {
        // TODO: implement
        self.fallback.object_cache_reader.get_latest_marker(object_id, epoch_id)
    }

    fn get_highest_pruned_checkpoint(&self) -> Option<CheckpointSequenceNumber> {
        self.fallback.object_cache_reader.get_highest_pruned_checkpoint()
    }

    fn notify_read_input_objects<'a>(&'a self, input_and_receiving_keys: &'a [InputKey], receiving_keys: &'a HashSet<InputKey>, epoch: EpochId) -> BoxFuture<'a, ()> {
        self.fallback.object_cache_reader.notify_read_input_objects(input_and_receiving_keys, receiving_keys,epoch)
    }
}