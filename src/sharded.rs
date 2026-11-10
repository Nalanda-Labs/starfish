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
        Sharded::try_start_with(move |shard| {
            let instance = factory(shard);
            async move { Ok::<T, Error>(instance) }
        })
        .await
    }

    /// Like [`start`](Self::start), with a factory that is a future: each
    /// instance may wait for things while it is being built, such as
    /// opening its files or recovering its data.
    ///
    /// ```no_run
    /// use starfish::fs::File;
    /// use starfish::Sharded;
    ///
    /// # async fn example() -> starfish::Result<()> {
    /// // One open file per shard.
    /// let files = Sharded::try_start_with(|shard| async move {
    ///     File::create(format!("/var/data/shard-{shard}")).await
    /// })
    /// .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// The futures run on their own shards, all at once.
    pub async fn start_with<F, Fut>(factory: F) -> Result<Sharded<T>>
    where
        F: Fn(usize) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = T> + 'static,
    {
        Sharded::try_start_with(move |shard| {
            let building = factory(shard);
            async move { Ok::<T, Error>(building.await) }
        })
        .await
    }

    /// Like [`start_with`](Self::start_with), with a factory that can
    /// fail.
    ///
    /// # Errors
    ///
    /// The first shard's error, if any factory failed, and
    /// [`Error::Panicked`] if one panicked. The service is then removed
    /// from the shards where it did start, so it is all or nothing.
    pub async fn try_start_with<F, Fut, E>(factory: F) -> Result<Sharded<T>>
    where
        F: Fn(usize) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<T, E>> + 'static,
        E: Into<Error> + 'static,
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
                let number = shard.shared.id;
                // Not held while the instance is being built: that may
                // take a while, and may use the shard itself.
                drop(shard);
                let instance: Rc<dyn Any> = Rc::new(factory(number).await.map_err(Into::into)?);
                let shard = current()?;
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

    /// Runs `finish` against every instance, then removes the instances.
    /// It is the place for what an instance must do before it goes:
    /// flush, close, wait for its work in progress.
    ///
    /// The instances are removed whether or not `finish` succeeded
    /// everywhere.
    ///
    /// # Errors
    ///
    /// The first shard's failure to run `finish`, such as its panic.
    pub async fn stop_with<F, Fut>(self, finish: F) -> Result<()>
    where
        F: Fn(Rc<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + 'static,
    {
        let finished = self.invoke_on_all(finish).await;
        self.stop().await?;
        finished.map(|_| ())
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

impl<T: 'static> std::fmt::Debug for Sharded<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sharded").field("id", &self.id).finish()
    }
}