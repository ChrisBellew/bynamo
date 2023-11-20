use futures::{future::select_all, Future};

pub async fn select_first_ok<F, T, E>(futures: Vec<F>) -> Result<T, Vec<E>>
where
    F: Future<Output = Result<T, E>> + std::marker::Unpin,
{
    let mut futures = futures.into_iter().collect::<Vec<_>>();
    while !futures.is_empty() {
        let (result, index, remaining) = select_all(futures).await;
        match result {
            Ok(value) => return Ok(value),
            Err(_) => {
                futures = remaining;
                futures.remove(index);
            }
        }
    }
    Err(vec![]) // or handle the case where all futures return Err
}
