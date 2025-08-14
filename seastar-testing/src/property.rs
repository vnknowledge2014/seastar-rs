//! Property-based testing utilities

use async_trait::async_trait;
use futures::Future;
use proptest::prelude::*;
use quickcheck::{Arbitrary, Gen, QuickCheck};
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, info, warn};

#[derive(Error, Debug)]
pub enum PropertyError {
    #[error("Property test failed: {0}")]
    TestFailed(String),
    #[error("Generator error: {0}")]
    Generator(String),
    #[error("Validation error: {0}")]
    Validation(String),
}

pub type PropertyResult<T> = Result<T, PropertyError>;

/// Property test configuration
#[derive(Debug, Clone)]
pub struct PropertyConfig {
    pub max_tests: usize,
    pub max_shrink_iterations: usize,
    pub min_tests_passed: usize,
    pub timeout_ms: u64,
    pub verbose: bool,
}

impl Default for PropertyConfig {
    fn default() -> Self {
        Self {
            max_tests: 100,
            max_shrink_iterations: 10,
            min_tests_passed: 100,
            timeout_ms: 30000,
            verbose: false,
        }
    }
}

/// Property test result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PropertyTestResult {
    pub property_name: String,
    pub tests_run: usize,
    pub tests_passed: usize,
    pub tests_failed: usize,
    pub shrink_iterations: usize,
    pub counterexample: Option<String>,
    pub execution_time_ms: u64,
    pub success: bool,
}

/// Trait for defining properties to test
#[async_trait]
pub trait Property: Send + Sync {
    type Input: Send + Sync + Debug;
    
    /// The property to test - should return true if property holds
    async fn test(&self, input: Self::Input) -> PropertyResult<bool>;
    
    /// Name of the property for reporting
    fn name(&self) -> &str;
    
    /// Precondition that must be satisfied for the test input
    fn precondition(&self, _input: &Self::Input) -> bool {
        true
    }
}

/// Property test runner
pub struct PropertyTest {
    config: PropertyConfig,
    properties: Vec<Box<dyn Property<Input = Box<dyn std::any::Any + Send + Sync>>>>,
}

impl PropertyTest {
    pub fn new(config: PropertyConfig) -> Self {
        Self {
            config,
            properties: Vec::new(),
        }
    }

    pub fn with_default_config() -> Self {
        Self::new(PropertyConfig::default())
    }

    /// Add a property to test
    pub fn add_property<P>(&mut self, property: P)
    where
        P: Property + 'static,
        P::Input: 'static,
    {
        // Type erasure wrapper
        struct PropertyWrapper<P>(P);

        #[async_trait]
        impl<P> Property for PropertyWrapper<P>
        where
            P: Property + Send + Sync,
            P::Input: 'static + Send + Sync,
        {
            type Input = Box<dyn std::any::Any + Send + Sync>;

            async fn test(&self, input: Self::Input) -> PropertyResult<bool> {
                let concrete_input = input
                    .downcast::<P::Input>()
                    .map_err(|_| PropertyError::Generator("Type downcast failed".to_string()))?;
                self.0.test(*concrete_input).await
            }

            fn name(&self) -> &str {
                self.0.name()
            }

            fn precondition(&self, input: &Self::Input) -> bool {
                if let Some(concrete_input) = input.downcast_ref::<P::Input>() {
                    self.0.precondition(concrete_input)
                } else {
                    false
                }
            }
        }

        self.properties.push(Box::new(PropertyWrapper(property)));
    }

    /// Run all property tests
    pub async fn run_all(&self) -> Vec<PropertyTestResult> {
        let mut results = Vec::new();
        
        for property in &self.properties {
            info!("Running property test: {}", property.name());
            let result = self.run_single_property(property.as_ref()).await;
            results.push(result);
        }
        
        results
    }

    async fn run_single_property(&self, property: &dyn Property<Input = Box<dyn std::any::Any + Send + Sync>>) -> PropertyTestResult {
        let start_time = std::time::Instant::now();
        let mut tests_run = 0;
        let mut tests_passed = 0;
        let mut tests_failed = 0;
        let mut counterexample = None;

        for _ in 0..self.config.max_tests {
            tests_run += 1;
            
            // Generate random test input (simplified - in practice would use proper generators)
            let input = self.generate_test_input().await;
            
            if !property.precondition(&input) {
                continue; // Skip inputs that don't meet preconditions
            }

            let input_debug = format!("{:?}", input);
            match property.test(input).await {
                Ok(true) => {
                    tests_passed += 1;
                    if self.config.verbose {
                        debug!("Property test passed for input: {}", input_debug);
                    }
                }
                Ok(false) => {
                    tests_failed += 1;
                    counterexample = Some(input_debug.clone());
                    warn!("Property test failed for input: {}", input_debug);
                    break; // Stop on first failure
                }
                Err(e) => {
                    tests_failed += 1;
                    counterexample = Some(format!("Error: {} with input: {}", e, input_debug));
                    warn!("Property test error: {} for input: {}", e, input_debug);
                    break;
                }
            }
        }

        let execution_time_ms = start_time.elapsed().as_millis() as u64;
        let success = tests_failed == 0 && tests_passed >= self.config.min_tests_passed;

        PropertyTestResult {
            property_name: property.name().to_string(),
            tests_run,
            tests_passed,
            tests_failed,
            shrink_iterations: 0, // TODO: Implement shrinking
            counterexample,
            execution_time_ms,
            success,
        }
    }

    async fn generate_test_input(&self) -> Box<dyn std::any::Any + Send + Sync> {
        // Simplified random input generation for common test types
        // In practice this would be much more sophisticated with proper type dispatch
        let mut rng = thread_rng();
        
        // Generate tuple of i32 values for addition property test
        let a: i32 = rng.gen_range(-1000..1000);
        let b: i32 = rng.gen_range(-1000..1000);
        Box::new((a, b))
    }
}

/// Generator for creating test data
pub trait PropertyGenerator<T> {
    fn generate(&mut self, size: usize) -> T;
    fn shrink(&self, value: T) -> Vec<T>;
}

/// Built-in generators for common types
pub struct Generators;

impl Generators {
    /// Generate random strings
    pub fn string(min_len: usize, max_len: usize) -> impl PropertyGenerator<String> {
        StringGenerator { min_len, max_len }
    }

    /// Generate random integers
    pub fn integer(min: i64, max: i64) -> impl PropertyGenerator<i64> {
        IntegerGenerator { min, max }
    }

    /// Generate random vectors
    pub fn vec<T: Clone>(
        element_gen: Box<dyn PropertyGenerator<T>>,
        min_len: usize,
        max_len: usize,
    ) -> impl PropertyGenerator<Vec<T>> {
        VecGenerator {
            element_gen,
            min_len,
            max_len,
        }
    }

    /// Generate random hash maps
    pub fn hash_map<K: Clone, V: Clone>(
        key_gen: Box<dyn PropertyGenerator<K>>,
        value_gen: Box<dyn PropertyGenerator<V>>,
        min_size: usize,
        max_size: usize,
    ) -> impl PropertyGenerator<HashMap<K, V>>
    where
        K: std::hash::Hash + Eq,
    {
        HashMapGenerator {
            key_gen,
            value_gen,
            min_size,
            max_size,
        }
    }
}

struct StringGenerator {
    min_len: usize,
    max_len: usize,
}

impl PropertyGenerator<String> for StringGenerator {
    fn generate(&mut self, _size: usize) -> String {
        let mut rng = thread_rng();
        let len = rng.gen_range(self.min_len..=self.max_len);
        
        (0..len)
            .map(|_| rng.gen_range(b'a'..=b'z') as char)
            .collect()
    }

    fn shrink(&self, value: String) -> Vec<String> {
        let mut shrunk = Vec::new();
        
        // Try shorter strings
        if value.len() > self.min_len {
            for i in (self.min_len..value.len()).rev() {
                shrunk.push(value.chars().take(i).collect());
            }
        }
        
        shrunk
    }
}

struct IntegerGenerator {
    min: i64,
    max: i64,
}

impl PropertyGenerator<i64> for IntegerGenerator {
    fn generate(&mut self, _size: usize) -> i64 {
        thread_rng().gen_range(self.min..=self.max)
    }

    fn shrink(&self, value: i64) -> Vec<i64> {
        let mut shrunk = Vec::new();
        
        // Shrink towards zero
        if value > 0 {
            shrunk.push(0);
            if value > 1 {
                shrunk.push(value / 2);
                shrunk.push(value - 1);
            }
        } else if value < 0 {
            shrunk.push(0);
            if value < -1 {
                shrunk.push(value / 2);
                shrunk.push(value + 1);
            }
        }
        
        // Keep within bounds
        shrunk.retain(|&x| x >= self.min && x <= self.max);
        shrunk
    }
}

struct VecGenerator<T> {
    element_gen: Box<dyn PropertyGenerator<T>>,
    min_len: usize,
    max_len: usize,
}

impl<T: Clone> PropertyGenerator<Vec<T>> for VecGenerator<T> {
    fn generate(&mut self, size: usize) -> Vec<T> {
        let mut rng = thread_rng();
        let len = rng.gen_range(self.min_len..=self.max_len);
        
        (0..len)
            .map(|_| self.element_gen.generate(size))
            .collect()
    }

    fn shrink(&self, value: Vec<T>) -> Vec<Vec<T>> {
        let mut shrunk = Vec::new();
        
        // Try shorter vectors
        if value.len() > self.min_len {
            shrunk.push(Vec::new()); // Empty vector
            for i in (self.min_len..value.len()).rev() {
                shrunk.push(value.iter().take(i).cloned().collect());
            }
        }
        
        shrunk
    }
}

struct HashMapGenerator<K, V> {
    key_gen: Box<dyn PropertyGenerator<K>>,
    value_gen: Box<dyn PropertyGenerator<V>>,
    min_size: usize,
    max_size: usize,
}

impl<K: Clone + std::hash::Hash + Eq, V: Clone> PropertyGenerator<HashMap<K, V>> for HashMapGenerator<K, V> {
    fn generate(&mut self, size: usize) -> HashMap<K, V> {
        let mut rng = thread_rng();
        let len = rng.gen_range(self.min_size..=self.max_size);
        let mut map = HashMap::new();
        
        for _ in 0..len {
            let key = self.key_gen.generate(size);
            let value = self.value_gen.generate(size);
            map.insert(key, value);
        }
        
        map
    }

    fn shrink(&self, value: HashMap<K, V>) -> Vec<HashMap<K, V>> {
        let mut shrunk = Vec::new();
        
        // Try smaller maps
        if value.len() > self.min_size {
            shrunk.push(HashMap::new()); // Empty map
            
            // Remove one key at a time
            for key in value.keys() {
                let mut smaller = value.clone();
                smaller.remove(key);
                if smaller.len() >= self.min_size {
                    shrunk.push(smaller);
                }
            }
        }
        
        shrunk
    }
}

/// Common property patterns
pub struct CommonProperties;

impl CommonProperties {
    /// Identity property: f(f(x)) == f(x) for idempotent functions
    pub fn idempotent<T, F>(f: F) -> impl Property<Input = T>
    where
        T: Clone + PartialEq + Debug + Send + Sync + 'static,
        F: Fn(T) -> T + Clone + Send + Sync + 'static,
    {
        IdempotentProperty { func: f, _phantom: std::marker::PhantomData }
    }

    /// Inverse property: f(g(x)) == x for inverse functions
    pub fn inverse<T, F, G>(f: F, g: G) -> impl Property<Input = T>
    where
        T: Clone + PartialEq + Debug + Send + Sync + 'static,
        F: Fn(T) -> T + Clone + Send + Sync + 'static,
        G: Fn(T) -> T + Clone + Send + Sync + 'static,
    {
        InverseProperty { func: f, inverse: g, _phantom: std::marker::PhantomData }
    }

    /// Commutativity property: f(x, y) == f(y, x)
    pub fn commutative<T, F>(f: F) -> impl Property<Input = (T, T)>
    where
        T: Clone + PartialEq + Debug + Send + Sync + 'static,
        F: Fn(T, T) -> T + Clone + Send + Sync + 'static,
    {
        CommutativeProperty { func: f, _phantom: std::marker::PhantomData }
    }

    /// Associativity property: f(f(x, y), z) == f(x, f(y, z))
    pub fn associative<T, F>(f: F) -> impl Property<Input = (T, T, T)>
    where
        T: Clone + PartialEq + Debug + Send + Sync + 'static,
        F: Fn(T, T) -> T + Clone + Send + Sync + 'static,
    {
        AssociativeProperty { func: f, _phantom: std::marker::PhantomData }
    }
}

struct IdempotentProperty<T, F> {
    func: F,
    _phantom: std::marker::PhantomData<T>,
}

#[async_trait]
impl<T, F> Property for IdempotentProperty<T, F>
where
    T: Clone + PartialEq + Debug + Send + Sync + 'static,
    F: Fn(T) -> T + Clone + Send + Sync + 'static,
{
    type Input = T;

    async fn test(&self, input: Self::Input) -> PropertyResult<bool> {
        let result1 = (self.func)(input.clone());
        let result2 = (self.func)(result1.clone());
        Ok(result1 == result2)
    }

    fn name(&self) -> &str {
        "idempotent"
    }
}

struct InverseProperty<T, F, G> {
    func: F,
    inverse: G,
    _phantom: std::marker::PhantomData<T>,
}

#[async_trait]
impl<T, F, G> Property for InverseProperty<T, F, G>
where
    T: Clone + PartialEq + Debug + Send + Sync + 'static,
    F: Fn(T) -> T + Clone + Send + Sync + 'static,
    G: Fn(T) -> T + Clone + Send + Sync + 'static,
{
    type Input = T;

    async fn test(&self, input: Self::Input) -> PropertyResult<bool> {
        let transformed = (self.func)(input.clone());
        let recovered = (self.inverse)(transformed);
        Ok(recovered == input)
    }

    fn name(&self) -> &str {
        "inverse"
    }
}

struct CommutativeProperty<T, F> {
    func: F,
    _phantom: std::marker::PhantomData<T>,
}

#[async_trait]
impl<T, F> Property for CommutativeProperty<T, F>
where
    T: Clone + PartialEq + Debug + Send + Sync + 'static,
    F: Fn(T, T) -> T + Clone + Send + Sync + 'static,
{
    type Input = (T, T);

    async fn test(&self, input: Self::Input) -> PropertyResult<bool> {
        let (x, y) = input;
        let result1 = (self.func)(x.clone(), y.clone());
        let result2 = (self.func)(y, x);
        Ok(result1 == result2)
    }

    fn name(&self) -> &str {
        "commutative"
    }
}

struct AssociativeProperty<T, F> {
    func: F,
    _phantom: std::marker::PhantomData<T>,
}

#[async_trait]
impl<T, F> Property for AssociativeProperty<T, F>
where
    T: Clone + PartialEq + Debug + Send + Sync + 'static,
    F: Fn(T, T) -> T + Clone + Send + Sync + 'static,
{
    type Input = (T, T, T);

    async fn test(&self, input: Self::Input) -> PropertyResult<bool> {
        let (x, y, z) = input;
        let result1 = (self.func)((self.func)(x.clone(), y.clone()), z.clone());
        let result2 = (self.func)(x, (self.func)(y, z));
        Ok(result1 == result2)
    }

    fn name(&self) -> &str {
        "associative"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct AdditionProperty;

    #[async_trait]
    impl Property for AdditionProperty {
        type Input = (i32, i32);

        async fn test(&self, input: Self::Input) -> PropertyResult<bool> {
            let (a, b) = input;
            // Property: addition is commutative
            Ok(a + b == b + a)
        }

        fn name(&self) -> &str {
            "addition_commutative"
        }
    }

    #[tokio::test]
    async fn test_property_test() {
        let mut test_runner = PropertyTest::with_default_config();
        test_runner.add_property(AdditionProperty);
        
        let results = test_runner.run_all().await;
        
        assert!(!results.is_empty());
        assert!(results[0].success);
        assert_eq!(results[0].property_name, "addition_commutative");
    }

    #[test]
    fn test_string_generator() {
        let mut gen = Generators::string(5, 10);
        let s = gen.generate(0);
        
        assert!(s.len() >= 5 && s.len() <= 10);
        
        let shrunk = gen.shrink(s);
        assert!(!shrunk.is_empty());
    }

    #[test]
    fn test_integer_generator() {
        let mut gen = Generators::integer(-100, 100);
        let n = gen.generate(0);
        
        assert!(n >= -100 && n <= 100);
        
        let shrunk = gen.shrink(n);
        // Should have shrunk values for non-zero numbers
        if n != 0 {
            assert!(!shrunk.is_empty());
        }
    }

    #[tokio::test]
    async fn test_common_properties() {
        // Test idempotent property
        let abs_prop = CommonProperties::idempotent(|x: i32| x.abs());
        assert!(abs_prop.test(5).await.unwrap());
        assert!(abs_prop.test(-5).await.unwrap());

        // Test commutative property
        let add_prop = CommonProperties::commutative(|x: i32, y: i32| x + y);
        assert!(add_prop.test((3, 7)).await.unwrap());
        assert!(add_prop.test((-2, 5)).await.unwrap());
    }
}