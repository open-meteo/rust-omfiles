use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

#[allow(private_bounds)]
pub trait TreeNavigator: TreeNavigatorInternal {
    /// Lazy child discovery with metadata caching
    fn get_child_by_name(&self, name: &str) -> Option<Self> {
        // Check cache first
        if let Some(cached_metadata) = self.cache().get_item(name) {
            return self.create_child_from_metadata(&cached_metadata);
        }

        // Search through children and cache metadata
        for i in 0..self.number_of_children() {
            if let Some(metadata) = self.get_child_metadata(i) {
                if let Some(child) = self.create_child_from_metadata(&metadata) {
                    if let Some(child_name) = child.get_name() {
                        self.cache().set_item(child_name.clone(), metadata.clone());

                        if child_name == name {
                            return Some(child);
                        }
                    }
                }
            }
        }
        None
    }

    fn find_by_path(&self, path: &str) -> Option<Self> {
        let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        self.navigate_path(&parts)
    }

    fn navigate_path(&self, parts: &[&str]) -> Option<Self> {
        if parts.is_empty() {
            return None;
        }

        if let Some(child) = self.get_child_by_name(parts[0]) {
            if parts.len() == 1 {
                Some(child)
            } else {
                child.navigate_path(&parts[1..])
            }
        } else {
            None
        }
    }
}

pub(crate) trait Cache {
    type CachedType;

    fn get_item(&self, string: &str) -> Option<Self::CachedType>;
    fn set_item(&self, string: String, value: Self::CachedType);
}

pub(crate) trait TreeNavigatorInternal: Sized {
    type ChildMetadata: Clone;
    type Cache: Cache<CachedType = Self::ChildMetadata>;

    fn get_name(&self) -> Option<String>;
    fn number_of_children(&self) -> u32;
    fn get_child_metadata(&self, index: u32) -> Option<Self::ChildMetadata>;
    fn create_child_from_metadata(&self, metadata: &Self::ChildMetadata) -> Option<Self>;
    fn cache(&self) -> &Self::Cache;
}

/// Simple HashMap-based cache for metadata
#[derive(Debug)]
pub struct MetadataCache<T> {
    map: Arc<Mutex<HashMap<String, T>>>,
}

impl<T> Clone for MetadataCache<T> {
    fn clone(&self) -> Self {
        Self {
            map: Arc::clone(&self.map),
        }
    }
}

impl<T: Clone> Cache for MetadataCache<T> {
    type CachedType = T;

    fn get_item(&self, key: &str) -> Option<Self::CachedType> {
        self.map.lock().unwrap().get(key).cloned()
    }

    fn set_item(&self, key: String, value: Self::CachedType) {
        self.map.lock().unwrap().insert(key, value);
    }
}

impl<T> Default for MetadataCache<T> {
    fn default() -> Self {
        Self {
            map: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}
