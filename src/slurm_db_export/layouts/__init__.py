"""Layouts: what ``export`` writes. One module per layout, each a pipeline you read top to bottom.

======================  ========================================  =================================
Layout                  Writes                                    Reads
======================  ========================================  =================================
``raw``                 slurmdbd's tables as stored, by month     the database
``jobs``                one row per job, decoded for analysis     a ``raw`` bundle
======================  ========================================  =================================
"""
