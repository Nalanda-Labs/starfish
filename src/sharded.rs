//! A service with one instance per shard.

use std::any::Any;
use std::future::Future;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::runtime::{current, drop_quietly};
use crate::smp::{submit_to, submit_to_all};

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

/// A handle to a service that has one independent instance on every shard.
///
/// The instances are built on their own shards and never leave them, so `T`
/// does not need to be `Send` or `Sync` and can use `Rc`, `Cell` and
/// `RefCell` freely. The handle itself is a small `Copy` id that can be sent
/// anywhere.
pub struct Sharded<T: 'static> {
    id: u64,
    _marker: PhantomData<fn() -> T>,
}

impl<T: 'static> Clone for Sharded<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: 'static> Copy for Sharded<T> {}

impl<T: 'static> Sharded<T> {
    /// Builds one instance on each shard by calling `factory` there with the
    /// shard's index.
    ///
    /// # Errors
    ///
    /// [`Error::Panicked`] if `factory` panicked on some shard; the service
    /// is then removed from the shards where it did start.
    pub async fn start<F>(factory: F) -> Result<Sharded<T>>
    where
        F: Fn(usize) -> T + Send + Sync + 'static,
    {
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let factory = Arc::new(factory);
        let service = Sharded {
            id,
            _marker: PhantomData,
        };
        let started = submit_to_all(move || {
            let factory = factory.clone();
            async move {
                let shard = current()?;
                let instance: Rc<dyn Any> = Rc::new(factory(shard.shared.id));
                let previous = shard.services.with(|services| services.insert(id, instance));
                drop(previous);
                Ok::<(), Error>(())
            }
        })
        .await
        .and_then(|results| results.into_iter().collect::<Result<Vec<()>>>());
        if let Err(err) = started {
            let _ = service.stop().await;
            return Err(err);
        }
        Ok(service)
    }

    /// The instance that lives on the current shard.
    ///
    /// # Errors
    ///
    /// [`Error::ServiceStopped`] if the service has been stopped, and
    /// [`Error::NoRuntime`] outside a runtime.
    pub fn local(&self) -> Result<Rc<T>> {
        let shard = current()?;
        let instance = shard
            .services
            .with(|services| services.get(&self.id).cloned())
            .flatten()
            .ok_or(Error::ServiceStopped)?;
        instance.downcast::<T>().map_err(|_| Error::ServiceStopped)
    }

    /// Runs `f` against the instance on `shard` and returns its result.
    pub async fn invoke_on<F, Fut, R>(&self, shard: usize, f: F) -> Result<R>
    where
        F: FnOnce(Rc<T>) -> Fut + Send + 'static,
        Fut: Future<Output = R> + 'static,
        R: Send + 'static,
    {
        let this = *self;
        submit_to(shard, move || async move {
            let instance = this.local()?;
            Ok(f(instance).await)
        })
        .await?
    }

    /// Runs `f` against every instance concurrently and returns the results
    /// in shard order.
    pub async fn invoke_on_all<F, Fut, R>(&self, f: F) -> Result<Vec<R>>
    where
        F: Fn(Rc<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R> + 'static,
        R: Send + 'static,
    {
        let this = *self;
        let f = Arc::new(f);
        submit_to_all(move || {
            let f = f.clone();
            async move {
                let instance = this.local()?;
                Ok(f(instance).await)
            }
        })
        .await?
        .into_iter()
        .collect()
    }

    /// Runs `map` against every instance and folds the results on the
    /// calling shard.
    pub async fn map_reduce<F, Fut, R, A>(
        &self,
        map: F,
        init: A,
        reduce: impl FnMut(A, R) -> A,
    ) -> Result<A>
    where
        F: Fn(Rc<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R> + 'static,
        R: Send + 'static,
    {
        Ok(self.invoke_on_all(map).await?.into_iter().fold(init, reduce))
    }

    /// Removes the instance from every shard. An instance is dropped once
    /// the last `Rc` to it (from [`local`](Self::local)) is gone.
    pub async fn stop(self) -> Result<()> {
        let id = self.id;
        submit_to_all(move || async move {
            if let Ok(shard) = current() {
                let instance = shard.services.with(|services| services.remove(&id));
                drop_quietly(instance);
            }
        })
        .await?;
        Ok(())
    }
}
