#!/usr/bin/env python2.7

import argparse
import sys

import yaml


def main():
    parser = argparse.ArgumentParser(description="Merge yaml config files")
    parser.add_argument(
        '--local', type=argparse.FileType('r'),
        required=True,
        help="Local, modified cassandra config file")
    parser.add_argument(
        '--new', type=argparse.FileType('r'),
        required=True,
        help="New cassandra config file")
    parser.add_argument(
        '--original', type=argparse.FileType('r'),
        help="Original, unmodified cassandra config file from prior release")
    parser.add_argument(
        '--output', type=argparse.FileType('w'),
        help="Output merged config here. Default (STDOUT)"
        )
    args = parser.parse_args()
    local = yaml.load(args.local)
    new = yaml.load(args.new)
    if args.original:
        orig = yaml.load(args.original)
    else:
        orig = None
    merged, questionable = merge_configs(local, new, orig)
    output = args.output if args.output else sys.stdout
    output.write(yaml.dump(merged))
    print  # in case output is to STDOUT

    if questionable:
        print >> sys.stderr, "=" * 70
        print >> sys.stderr, \
            "\nWARNING: Some configuration keys had conflicting values!\n" \
            "         Please check these manually!\n"
        print >> sys.stderr, "=" * 70
        for key, values in sorted(questionable.items()):
            print >> sys.stderr, key
            lv, nv = values
            print >> sys.stderr, "    local: ", lv
            print >> sys.stderr, "    new:   ", nv
        print >> sys.stderr
        print >> sys.stderr, '-' * 70


def merge_configs(local, new, orig=None):
    if orig is None:
        orig = {}
    merged = {}
    questionable = {}

    in_both = local.viewkeys() & new.viewkeys()
    in_all = in_both & orig.viewkeys()
    in_both_only = in_both - in_all
    local_only = local.viewkeys() - in_both
    new_only = new.viewkeys() - in_both

    for key in local_only:
        merged[key] = local[key]
    for key in new_only:
        merged[key] = new[key]

    for key in in_both_only:
        l_val = local[key]
        n_val = new[key]
        l_type = type(l_val)
        n_type = type(n_val)

        if l_val == n_val:
            merged[key] = l_val
            continue

        if l_type is list and n_type is list:
            merged[key] = l_val[:]
            for ix, val in enumerate(n_val):
                if type(val) is dict:
                    try:
                        mval, mqs = merge_configs(l_val[ix], val)
                        merged[key].append(mval)
                        for q, v in mqs.items():
                            questionable["%s.%s.%s" % (key, ix, q)] = v
                    except IndexError:
                        merged[key].append(val)
                        questionable["%s.%s" % (key, ix)] = [None, val]
                elif val not in l_val:
                    merged[key].append(val)
                    questionable["%s.%s" % (key, len(merged[key]) - 1)] = \
                        [None, val]
        elif l_type is dict and n_type is dict:
            mkey, qkey = merge_configs(l_val, n_val)
            merged[key] = mkey
            for q, v in qkey.items():
                questionable["%s.%s" % (key, q)] = v
        else:
            merged[key] = l_val
            questionable[key] = [l_val, n_val]

    for key in in_all:
        l_val = local[key]
        n_val = new[key]
        o_val = orig[key]

        if o_val == l_val:
            merged[key] = n_val
        else:
            merged[key] = l_val
    return merged, questionable


# TODO(?)
#
# complex
#

# we need more info than will be included in a 'loaded' yaml doc
# so scan the stream and record not just the key/val pairs but *where*
# they occur in the document, and separately record the contents of the
# lines in between

# comparing 2 of these should produce a list of changes -- moved keys,
# changed comments/non-key blocks, changed key or subkey values

# then we need to figure out how to merge those changes from one into the other
# and then how to output a valid yaml file that represents that merge


if __name__ == '__main__':
    main()
