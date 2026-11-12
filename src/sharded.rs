//! A service with one instance per shard.

use std::any::Any;
use std::future::Future;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::runtime::{current, drop_quietly};
use crate::smp::{submit_to, submit_to_all, submit_to_each};

/// The number given to the next service to be started. Every service gets a
/// number of its own, unique in the process, under which each shard files its
/// instance. A [`Sharded`] handle is nothing more than that number.
static NEXT_ID: AtomicU64 = AtomicU64::new(0);

/// A handle to a service that has one independent instance on every shard.
///
/// This is how state is shared out in a shard-per-core program: instead of one
/// structure behind a lock, every shard has its own copy of the service, with
/// its own part of the data, and never touches another's. A request that
/// concerns another shard's part is sent there
/// ([`invoke_on`](Self::invoke_on)).
///
/// The instances are built on their own shards and never leave them, so `T`
/// does not need to be `Send` or `Sync` and can use `Rc`, `Cell` and
/// `RefCell` freely. The handle itself is a small `Copy` id that can be sent
/// anywhere, captured in closures and kept in other services.
///
/// ```no_run
/// use std::cell::Cell;
/// use starfish::Sharded;
///
/// # async fn example() -> starfish::Result<()> {
/// let hits = Sharded::start(|_shard| Cell::new(0u64)).await?;
/// // This shard's instance, with no message passing:
/// hits.local()?.set(1);
/// // Every instance, each on its own shard:
/// hits.invoke_on_all(|hits| async move { hits.set(hits.get() + 1) }).await?;
/// hits.stop().await?;
/// # Ok(())
/// # }
/// ```
///
/// A service lives until [`stop`](Self::stop) or
/// [`stop_with`](Self::stop_with), or until the runtime ends.
pub struct Sharded<T: 'static> {
    id: u64,
    _marker: PhantomData<fn() -> T>,
}

impl<T: 'static> Clone for Sharded<T> {
    /// Copies the handle. The copy refers to the same service: there is still
    /// one instance per shard, however many handles there are.
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
    /// This is an ordinary lookup in the shard's own table, with no message and
    /// no waiting, so it is the way to reach the service from code already
    /// running on the right shard. The returned `Rc` keeps the instance alive
    /// even if the service is stopped meanwhile; do not hold it longer than the
    /// work at hand.
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

    /// Runs `f` against the instance on shard `shard` and returns its result.
    ///
    /// The closure is sent to that shard, called there with the instance, and the
    /// future it returns runs there to completion. Its result is sent back. The
    /// closure and the result cross threads and must be `Send`; the future does
    /// not, and may hold the `Rc` and anything else that belongs to that shard.
    /// If `shard` is the current shard, the closure simply runs in place.
    ///
    /// # Errors
    ///
    /// Those of [`submit_to`](crate::submit_to): [`Error::InvalidShard`],
    /// [`Error::Panicked`] if `f` panicked, [`Error::Canceled`] if that shard
    /// shut down first. Also [`Error::ServiceStopped`] if the service is no
    /// longer running there.
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

    /// Runs `f` against every instance, each on its own shard and all at the
    /// same time, and returns the results in shard order.
    ///
    /// `f` is called once per shard, so it must be `Fn`, and it is shared between
    /// threads, so it must be `Send` and `Sync`. What it captures is cloned or
    /// borrowed per call, as for any `Fn`.
    ///
    /// # Errors
    ///
    /// The first failure among the shards, as for
    /// [`submit_to_all`](crate::submit_to_all); the closure has still run on the
    /// shards that did not fail.
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

    /// Runs `f` against every instance, each on its own shard and all at the
    /// same time, and returns every shard's outcome in shard order: its
    /// result, or why there is none.
    ///
    /// Where [`invoke_on_all`](Self::invoke_on_all) gives up at the first
    /// failure and discards the rest, this says what happened on each shard.
    /// Use it when the calls have effects and it matters which of them took
    /// place, or when one shard failing should not hide the others' answers.
    ///
    /// ```no_run
    /// # use std::cell::Cell;
    /// # async fn example(hits: starfish::Sharded<Cell<u64>>) -> starfish::Result<()> {
    /// let outcomes = hits.invoke_on_each(|hits| async move { hits.get() }).await?;
    /// for (shard, outcome) in outcomes.iter().enumerate() {
    ///     match outcome {
    ///         Ok(count) => println!("shard {shard}: {count}"),
    ///         Err(error) => println!("shard {shard} failed: {error}"),
    ///     }
    /// }
    /// # Ok(()) }
    /// ```
    ///
    /// # Errors
    ///
    /// The call as a whole fails only with [`Error::NoRuntime`], outside a
    /// runtime. Every other failure is one shard's, and is in its place in
    /// the list: [`Error::Panicked`] if `f` panicked there,
    /// [`Error::Canceled`] if the shard shut down first,
    /// [`Error::ServiceStopped`] if the service is no longer running there.
    pub async fn invoke_on_each<F, Fut, R>(&self, f: F) -> Result<Vec<Result<R>>>
    where
        F: Fn(Rc<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R> + 'static,
        R: Send + 'static,
    {
        let this = *self;
        let f = Arc::new(f);
        let outcomes = submit_to_each(move || {
            let f = f.clone();
            async move {
                let instance = this.local()?;
                Ok(f(instance).await)
            }
        })
        .await?;
        Ok(outcomes
            .into_iter()
            .map(|outcome| outcome.and_then(|inner| inner))
            .collect())
    }

    /// Runs `map` against every instance, then combines the results on the
    /// calling shard.
    ///
    /// `map` runs on each shard as for [`invoke_on_all`](Self::invoke_on_all).
    /// `reduce` then runs here, starting from `init` and taking the results in
    /// shard order. It is the way to compute a total, a maximum or a merged list
    /// over all the shards:
    ///
    /// ```no_run
    /// # use std::cell::Cell;
    /// # async fn example(hits: starfish::Sharded<Cell<u64>>) -> starfish::Result<u64> {
    /// hits.map_reduce(|hits| async move { hits.get() }, 0, |sum, one| sum + one).await
    /// # }
    /// ```
    ///
    /// `reduce` runs on one shard only, so unlike `map` it needs neither `Send`
    /// nor `Sync`.
    ///
    /// # Errors
    ///
    /// As for [`invoke_on_all`](Self::invoke_on_all).
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

    /// Removes the instance from every shard.
    ///
    /// Each shard drops its reference. An instance is destroyed once the last
    /// `Rc` to it is gone, which is at once unless a task still holds one from
    /// [`local`](Self::local) or is in the middle of an `invoke_on` call. Nothing
    /// is called on the instances; to have them finish their work first, use
    /// [`stop_with`](Self::stop_with).
    ///
    /// After this, every method of every copy of the handle returns
    /// [`Error::ServiceStopped`].
    ///
    /// # Errors
    ///
    /// Those of [`submit_to_all`](crate::submit_to_all), in the unlikely case
    /// that a shard cannot be reached.
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
    /// Prints the handle as `Sharded { id: .. }`. The instances live on their
    /// shards and are not shown.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sharded").field("id", &self.id).finish()
    }
}