﻿using System;

namespace Raven.Server.Documents
{
    [Flags]
    public enum DocumentFlags
    {
        None = 0,

        Artificial = 0x1,
        HasRevisions = 0x2,
        DeleteRevision = 0x4,
        Reserved2 = 0x8,

        FromIndex = 0x10,
        Revision = 0x20,
        FromReplication = 0x40,
        Reserved3 = 0x80,

        HasAttachments = 0x100,
        Resolved = 0x200,
        Conflicted = 0x400
    }

    [Flags]
    public enum NonPersistentDocumentFlags
    {
        None = 0,

        LegacyRevision = 0x1,
        LegacyHasRevisions = 0x2,
        FromSmuggler = 0x4,
        FromReplication = 0x8,
        ByAttachmentUpdate = 0x10,
        ResolveAttachmentsConflict = 0x20,
        FromRevision = 0x40,
        Resolved = 0x80,
        SkipRevisionCreation = 0x100
    }

    public static class EnumExtensions
    {
        public static bool Contain(this DocumentFlags current, DocumentFlags flag)
        {
            return (current & flag) == flag;
        }

        public static bool Contain(this NonPersistentDocumentFlags current, NonPersistentDocumentFlags flag)
        {
            return (current & flag) == flag;
        }
    }
}
