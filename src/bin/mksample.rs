//! This program creates bounded random sample text for use in benchmarking `groupby`. It accepts
//! no command-line inputs; all customization is performed through the `main` function. Thus, to
//! generate all necessary sample files for benchmarking, simply run this program once.

use std::collections::VecDeque;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufWriter;
#[allow(unused_imports)]
use std::iter;
use std::ops::Range;
use std::thread::{self, JoinHandle};

/// Specifies the bounds for the lengths of lines in a sample file (excluding the newline character).
pub enum LineLength {
    /// Each line will be exactly this many characters.
    Fixed(usize),

    /// Each line's length will be randomly chosen from this range.
    Range(Range<usize>),
}

/// Specifies the length of the output file as either a number of lines or a number of characters
/// (including newlines).
pub enum SampleLength {
    Lines(usize),
    Characters(usize),
}

/// Builds all preconfigured samples.
fn main() {
    let cg = fastrand::alphanumeric;

    // TODO Build out the sample set once we're ready to build charts.

    // Example builds....

    let mut builder = SampleBuilder::new();
    builder.sample(
        "fixed-20char-30MB-alphanumeric.txt",
        LineLength::Fixed(20),
        SampleLength::Characters(30_000_000),
        cg,
    );

    builder.sample(
        "ranged-5to80char-30MB-alphanumeric.txt",
        LineLength::Range(5..81),
        SampleLength::Characters(30_000_000),
        cg,
    );

    builder.sample(
        "ranged-5to80char-300MB-alphanumeric.txt",
        LineLength::Range(5..81),
        SampleLength::Characters(300_000_000),
        cg,
    );
}

/// Wraps build_sample invocations in new threads for easy parallelism.
//
// Note that this struct is not unit-tested. It's simple, the type system does most of the work,
// and testing it would require things like dependency injection to verify output to stdout and
// stderr. It's not worth the effort for this particular struct, since it's not used in any
// larger, production context where security issues could come into play, nor is it in a library.
#[derive(Default)]
pub struct SampleBuilder {
    samples: VecDeque<Sample>,
}

/// Holds the file handle and filename for a sample that's being built.
struct Sample {
    handle: JoinHandle<()>,
    filename: &'static str,
}

/// We implement Drop so we can automatically join all threads when the struct is dropped.
impl Drop for SampleBuilder {
    fn drop(&mut self) {
        while let Some(sample) = self.samples.pop_front() {
            match sample.handle.join() {
                Ok(_) => println!("Created sample: {}", sample.filename),
                Err(e) => eprintln!("{:?}", e),
            }
        }
    }
}

impl SampleBuilder {
    pub fn new() -> Self {
        SampleBuilder {
            samples: VecDeque::new(),
        }
    }

    /// Builds a sample in a new thread.
    pub fn sample(
        &mut self,
        filename: &'static str,
        line_length: LineLength,
        sample_length: SampleLength,
        character_generator: fn() -> char,
    ) {
        let handle = thread::spawn(move || {
            build_sample(
                File::create(filename).unwrap(),
                line_length,
                sample_length,
                character_generator,
            )
        });
        self.samples.push_back(Sample { handle, filename });
    }
}

/// Builds a sample based on the provided parameters and writes it to `file`.
pub fn build_sample(
    file: impl Write,
    line_length: LineLength,
    sample_length: SampleLength,
    character_generator: impl Fn() -> char,
) {
    // Let's buffer our writer, since we'll make lots of small writes.
    let mut file = BufWriter::new(file);

    match sample_length {
        SampleLength::Lines(n) => {
            for _ in 0..n {
                let line = build_line(&line_length, &character_generator);
                file.write_all(line.string.as_bytes()).unwrap();
            }
        }
        SampleLength::Characters(limit) => {
            let mut chars_written = 0;

            // Calculate the upper bound on the length of a line so that we can handle the last
            // line specially and ensure that we get the overall file length just right.
            let max_line_length = match line_length {
                LineLength::Fixed(n) => n + 1,     // +1 for newline.
                LineLength::Range(ref r) => r.end, // Range is half open, so no need for +1.
            };

            // Be careful not to subtract from limit here or you'll get subtract with overflow.
            while chars_written + max_line_length < limit {
                let line = build_line(&line_length, &character_generator);
                chars_written += line.length;
                file.write_all(line.string.as_bytes()).unwrap();
            }

            // Write the last line.
            if chars_written < limit {
                let line = build_line(
                    &LineLength::Fixed(limit - chars_written - 1),
                    &character_generator,
                );
                file.write_all(line.string.as_bytes()).unwrap();
            }
        }
    }

    // For safety.
    file.flush().unwrap();
}

/// Returned from `build_line`.
pub struct Line {
    /// A fully formed line (including newline).
    pub string: String,

    /// The length of the line in chars (including newline).
    pub length: usize,
}

/// Builds a line based on the provided parameters.
///
/// # Panics
///
/// Panics if given a `LineLength::Range(r)` where `r` is empty, e.g. `0..0` or `6..6`.
pub fn build_line(line_length: &LineLength, character_generator: &impl Fn() -> char) -> Line {
    let mut string: String;
    let length: usize;
    match line_length {
        LineLength::Fixed(n) => {
            string = String::with_capacity(n + 1);
            length = *n + 1;
            for _ in 0..*n {
                string.push(character_generator());
            }
        }
        LineLength::Range(r) => {
            assert_ne!(r.start, r.end);

            // The length of the line, including newline.
            length = fastrand::usize(r.clone()) + 1;

            string = String::with_capacity(length);
            for _ in 0..(length - 1) {
                string.push(character_generator());
            }
        }
    }
    string.push('\n');

    Line { string, length }
}

#[cfg(test)]
mod build_line_tests {
    use super::*;

    const CG_CHAR: char = 'c';

    fn cg() -> char {
        CG_CHAR
    }

    #[test]
    fn with_fixed_length_works() {
        let line = build_line(&LineLength::Fixed(5), &cg);
        assert_eq!(line.string, "ccccc\n");
        assert_eq!(line.length, 6);
        assert_eq!(line.string.len(), line.length); // Sanity check.
    }

    #[test]
    fn with_fixed_length_0_works() {
        let line = build_line(&LineLength::Fixed(0), &cg);
        assert_eq!(line.string, "\n");
        assert_eq!(line.length, 1);
        assert_eq!(line.string.len(), line.length); // Sanity check.
    }

    #[test]
    fn with_range_length_works() {
        // We can't actually definitively test this, since there's intentional randomness, so we'll
        // generate multiple lines and verify that they're all in-range.
        let range = 6..12;
        let tries = 100;
        for _ in 0..tries {
            let line = build_line(&LineLength::Range(range.clone()), &cg);

            // Verify that the number of CG_CHAR characters is within range.
            let cg_char_count = line.string.matches(CG_CHAR).count();
            assert!(range.start <= cg_char_count);
            assert!(cg_char_count < range.end);

            // Verify that length is correct.
            assert_eq!(cg_char_count + 1, line.length);
            assert_eq!(line.string.len(), line.length); // Sanity check.

            // Verify that there's a newline at the end. If so, the string must consist of k
            // repetitions of CG_CHAR followed by a single '\n' (for some k in range).
            assert_eq!('\n', line.string.chars().last().unwrap());
        }
    }

    #[test]
    #[should_panic]
    fn with_empty_range_length_panics() {
        build_line(&LineLength::Range(10..10), &cg);
    }
}

/// Testing for `build_sample` is much simpler than it might seem. We don't need to test all
/// permutations of `line_length`, nor do we need to meaningfully test `character_generator`,
/// because we don't actually use them in `build_sample`, we just pass them through to
/// `build_line`, where they're used and tested. We really just need to test our `sample_length`
/// processing with cursory checks that `build_line` is being called appropriately.
#[cfg(test)]
mod build_sample_tests {
    use super::*;

    const CG_CHAR: char = 'c';

    fn cg() -> char {
        CG_CHAR
    }

    #[test]
    fn with_sample_length_lines_works() {
        let mut sample = vec![];
        build_sample(
            &mut sample,
            LineLength::Fixed(3),
            SampleLength::Lines(3),
            &cg,
        );

        let expected: Vec<u8> = "ccc\nccc\nccc\n".bytes().collect();
        assert_eq!(expected, sample);
    }

    #[test]
    fn with_sample_length_lines_0_works() {
        let mut sample = vec![];
        build_sample(
            &mut sample,
            LineLength::Fixed(3),
            SampleLength::Lines(0),
            &cg,
        );

        let expected: Vec<u8> = vec![];
        assert_eq!(expected, sample);
    }

    #[test]
    fn with_sample_length_lines_and_large_value_works() {
        let line_length = 200;
        let line_count = 1_000;
        let mut sample = vec![];
        build_sample(
            &mut sample,
            LineLength::Fixed(line_length),
            SampleLength::Lines(line_count),
            &cg,
        );

        let mut line: String = iter::repeat("c").take(line_length).collect();
        line.push('\n');
        let expected: String = iter::repeat(line).take(line_count).collect();
        assert_eq!(expected.as_bytes(), sample);
    }

    #[test]
    fn with_sample_length_characters_works() {
        let mut sample = vec![];
        build_sample(
            &mut sample,
            LineLength::Fixed(3),
            SampleLength::Characters(12),
            &cg,
        );

        let expected: Vec<u8> = "ccc\nccc\nccc\n".bytes().collect();
        assert_eq!(expected, sample);
    }

    #[test]
    fn with_sample_length_characters_0_works() {
        let mut sample = vec![];
        build_sample(
            &mut sample,
            LineLength::Fixed(3),
            SampleLength::Characters(0),
            &cg,
        );

        let expected: Vec<u8> = vec![];
        assert_eq!(expected, sample);
    }

    #[test]
    fn with_uneven_last_line_length_matches_size_precisely() {
        let char_count = 31;
        let line_length = 7;
        let mut sample = vec![];
        build_sample(
            &mut sample,
            LineLength::Fixed(line_length),
            SampleLength::Characters(char_count),
            &cg,
        );
        assert_eq!(char_count, sample.len());
    }

    #[test]
    fn with_sample_length_characters_and_large_value_works() {
        let line_length = 200;
        let line_count = 1_000;
        let jagged_last_line = "ccccc\n";
        let char_count = (line_length + 1) * line_count + jagged_last_line.len();
        let mut sample = vec![];
        build_sample(
            &mut sample,
            LineLength::Fixed(line_length),
            SampleLength::Characters(char_count),
            &cg,
        );

        let mut line: String = iter::repeat("c").take(line_length).collect();
        line.push('\n');
        let mut expected: String = iter::repeat(line).take(line_count).collect();
        expected.push_str(jagged_last_line);
        assert_eq!(expected.as_bytes(), sample);
    }
}
