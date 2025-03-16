use std::{ffi::c_int, ops::{Bound, RangeBounds}, ptr};

use bitflags::bitflags;

use crate::{key::RedisKey, redis_error, redisraw::bindings::*, KeyType, RedisError, RedisResult, RedisString, Status};

bitflags! {
    #[derive(Default)]
    pub struct ZAddFlags: c_int {
        // Element must already exist. Do nothing otherwise
        const XX = REDISMODULE_ZADD_XX as c_int;
        // Element must not exist. Do nothing otherwise
        const NX = REDISMODULE_ZADD_NX as c_int;
        // If element exists, new score must be greater than the current score. 
        // Do nothing otherwise. Can optionally be combined with XX.
        const GT = REDISMODULE_ZADD_GT as c_int;
        // If element exists, new score must be less than the current score.
        // Do nothing otherwise. Can optionally be combined with XX.
        const LT = REDISMODULE_ZADD_LT as c_int;
    }
}

pub enum ZAddResult {
    Added,
    Updated,
    Nop,
}

// Performs `ZRANGE BYSCORE` on range bounds. Unbounded range is unsupported
pub struct ZSetScoreIterator<'a> {
    key: &'a RedisKey,
}

impl<'a> ZSetScoreIterator<'a> {
    pub(super) fn new(key: &'a RedisKey, range: impl RangeBounds<f64>, last: bool) -> RedisResult<Self> {
        if key.key_type() != KeyType::ZSet {
           return Err(RedisError::WrongType);
        }

        let (min, minex) = extract_bound(range.start_bound())?;
        let (max, maxex) = extract_bound(range.end_bound())?;

        let status: Status = unsafe {
            let init = match last {
                true => RedisModule_ZsetLastInScoreRange.unwrap(),
                false => RedisModule_ZsetFirstInScoreRange.unwrap(),
            };
            init(key.key_inner, min, max, minex.into(), maxex.into()).into()
        };
        match status {
            Status::Ok => Ok(Self{ key }),
            Status::Err => redis_error!("failed to create ZSet iterator"),
        }
    }
}

impl<'a> Iterator for ZSetScoreIterator<'a> {
    type Item = RedisString;

    fn next(&mut self) -> Option<Self::Item> {
        if unsafe { RedisModule_ZsetRangeEndReached.unwrap()(self.key.key_inner) } == 1 {
            return None;
        }
        let item_ptr = unsafe { RedisModule_ZsetRangeCurrentElement.unwrap()(self.key.key_inner, ptr::null_mut()) };
        let item = RedisString::from_redis_module_string(self.key.ctx, item_ptr);
        unsafe { RedisModule_ZsetRangeNext.unwrap()(self.key.key_inner) };
        Some(item)
    }
}

impl<'a> DoubleEndedIterator for ZSetScoreIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if unsafe { RedisModule_ZsetRangeEndReached.unwrap()(self.key.key_inner) } == 1 {
            return None;
        }
        let item_ptr = unsafe { RedisModule_ZsetRangeCurrentElement.unwrap()(self.key.key_inner, ptr::null_mut()) };
        let item = RedisString::from_redis_module_string(self.key.ctx, item_ptr);
        unsafe { RedisModule_ZsetRangePrev.unwrap()(self.key.key_inner) };
        Some(item)
    }
}

impl<'a> Drop for ZSetScoreIterator<'a> {
    fn drop(&mut self) {
       unsafe { RedisModule_ZsetRangeStop.unwrap()(self.key.key_inner) }
    }
}

// Returned bool indicates if bound is excluded
fn extract_bound(bound: Bound<&f64>) -> RedisResult<(f64, bool)> {
    match bound {
        Bound::Included(value) => Ok((*value, false)),
        Bound::Excluded(value) => Ok((*value, true)),
        Bound::Unbounded => redis_error!("unbounded range is unsupported"),
    }
}
